# encoding: UTF-8

require 'open-uri'
require 'mongo'
require 'json'
require 'zlib'
require 'uri'
require 'yaml'
require 'rubygems'
require 'net/ftp'

module HerokuMongoBackup
  if defined?(Rails::Railtie)
    class Railtie < Rails::Railtie
      rake_tasks do
        load "tasks/heroku_mongo_backup.rake"
      end
    end
  end

  require 's3_helpers'

  class Backup
    def env(list, required=true)
      var = list.map {|v| ENV[v]}.compact.first
      return var unless var.nil? and required

      if list.length == 1
        raise "ERROR: Environment variable #{list.first} must be set"
      else
        raise "ERROR: One of these environment variables must be set: #{list}"
      end
    end

    def chdir
      Dir.chdir("/tmp")
      begin
        Dir.mkdir("dump")
      rescue
      end
      Dir.chdir("dump")
    end

    def store
      backup = {}
  
      @db.collections.each do |col|
        backup['system.indexes.db.name'] = col.db.name if col.name == "system.indexes"
    
        records = []
    
        col.find().each do |record|
          records << record
        end

        backup[col.name] = records
      end
  
      marshal_dump = Marshal.dump(backup)
  
      file = File.new(@file_name, 'w')
      file.binmode
      file = Zlib::GzipWriter.new(file)
      file.write marshal_dump
      file.close
    end

    def load
      data = Zlib::GzipReader.new(StringIO.new(open(@file_name).read)).read
      obj = Marshal.load(data)

      obj.each do |col_name, records|
        next if col_name =~ /^system\./
    
        @db.drop_collection(col_name)
        dest_col = @db.create_collection(col_name)
    
        records.each do |record|
          dest_col.insert record
        end
      end
  
      # Load indexes here
      col_name = "system.indexes"
      dest_index_col = @db.collection(col_name)
      obj[col_name].each do |index|
        if index['_id']
          index['ns'] = index['ns'].sub(obj['system.indexes.db.name'], dest_index_col.db.name)
          dest_index_col.insert index
        end
      end
    end

    def db_connect
      uri = URI.parse(@url)
      connection = ::Mongo::Connection.new(uri.host, uri.port)
      @db = connection.db(uri.path.gsub(/^\//, ''))
      @db.authenticate(uri.user, uri.password) if uri.user
    end
    
    def ftp_connect
      @ftp = Net::FTP.new(env(['FTP_HOST']))
      @ftp.passive = true
      @ftp.login(env(['FTP_USERNAME']), env(['FTP_PASSWORD']))
    end

    def ftp_list
      @ftp.list()
    end
    
    def ftp_upload
      @ftp.putbinaryfile(@file_name)
    end
    
    def ftp_download
      open(@file_name, 'w') do |file|
        file_content = @ftp.getbinaryfile(@file_name)
        file.binmode
        file.write file_content
      end
    end
    
    def s3_connect
      # The first non-nil environment variable of each type will be used
      env_var_options = {
        :bucket_name       => %W[S3_BACKUPS_BUCKET S3_BACKUP_BUCKET S3_BACKUP S3_BUCKET],
        :access_key_id     => %W[S3_KEY_ID S3_KEY S3_ACCESS_KEY AWS_ACCESS_KEY_ID],
        :secret_access_key => %W[S3_SECRET_KEY S3_SECRET AWS_SECRET_ACCESS_KEY],
      }
      args = {}

      env_var_options.each do |type, list|
        args[type] = env(env_var_options[type])
      end

      @bucket = HerokuMongoBackup::s3_connect(args)
    end

    def s3_upload
      file = HerokuMongoBackup::s3_upload(@bucket, @file_name)
      puts file.public_url
      file
    end

    def s3_download
      open(@file_name, 'w') do |file|
        file_content = HerokuMongoBackup::s3_download(@bucket, @file_name)
        file.binmode
        file.write file_content
      end
    end

    def http_download(url)
      require 'open-uri'
      open(url.split('/').last, 'w') do |file|
        file << open(url).read
      end
    end

    def initialize connect = true
      @file_name = Time.now.strftime("%Y-%m-%d_%H-%M-%S.gz")
      @file_pattern = %r/\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}.gz/
  
      environment = env(%W[RAILS_ENV RACK_ENV], false)
      if ['production', 'staging'].include?(environment)
        #config_template = ERB.new(IO.read("config/mongoid.yml"))
        #uri = YAML.load(config_template.result)['production']['uri']
        uri = env(%W[MONGO_URL MONGOLAB_URI MONGOHQ_URL])
      else
        mongoid_config  = YAML.load_file("config/mongoid.yml")
        if mongoid_config['defaults'].exists? # mongoid version 2.x
          config = (mongoid_config['defaults']||{}).merge(mongoid_config[environment]||{})
          host       = config['host']
          port       = config['port']
          database   = config['database']
          uri = "mongodb://#{host}:#{port}/#{database}"
        else # mongoid version 3.x
          config     = mongoid_config[environment]['sessions']['default']
          host_port  = config['hosts'].first
          database   = config['database']
          uri = "mongodb://#{host_port}/#{database}"
        end
      end
  
      @url = uri
  
      self.db_connect

      if connect
        if ENV['UPLOAD_TYPE'] == 'ftp'
          self.ftp_connect
        else
          self.s3_connect
        end
      end
    end

    def most_recent_backup
      if ENV['UPLOAD_TYPE'] == 'ftp'
        last = @ftp.list.map {|s| s.split(/\s+/).last}.select {|f| f =~ @file_pattern}.sort.last
      else
        files = HerokuMongoBackup::s3_list(@bucket)
        matching, last = nil, nil
        files.each do |f|
          # This depends on the file list enumerator already being sorted, but
          # allows us to not iterate through every file in S3
          url = f.public_url.to_s
          break if matching and url !~ %r{backups/#{@file_pattern}}
          matching = url =~ %r{backups/#{@file_pattern}}
          last = url if matching
        end
      end
      puts last
    end

    def backup files_number_to_leave=0
      self.chdir    
      self.store

      if ENV['UPLOAD_TYPE'] == 'ftp'
        self.ftp_upload
        @ftp.close
      else
        self.s3_upload
      end

      if files_number_to_leave > 0
        HerokuMongoBackup::remove_old_backup_files(@bucket, files_number_to_leave)
      end
    end
    
    def restore file_name, download_file = true
      @file_name = file_name
  
      self.chdir
      
      if download_file
        if @file_name =~ /^http/
        elsif ENV['UPLOAD_TYPE'] == 'ftp'
          self.ftp_download
          @ftp.close
        elsif 
          self.s3_download
        end
      end

      self.load
    end
  end
end
