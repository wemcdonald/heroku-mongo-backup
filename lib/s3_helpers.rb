require 'open-uri'

gems = [ENV['S3_GEM'], 'fog', 'aws/s3', 's3'].compact
chosen_gem = nil

gems.any? do |name|
  begin
    require name
    chosen_gem = name
  rescue LoadError
  end
end

case chosen_gem
when 's3'
  def HerokuMongoBackup::s3_connect(args)
    service = S3::Service.new(*args.slice(:access_key_id, :secret_access_key))
    service.buckets.find(args[:bucket_name])
  end

  def HerokuMongoBackup::s3_list(directory)
    directory.objects
  end

  def HerokuMongoBackup::s3_upload(bucket, filename)
    object = bucket.objects.build("backups/#{filename}")
    object.content = open(filename)
    object.save
  end

  def HerokuMongoBackup::s3_download(bucket, filename)
    object  = bucket.objects.find("backups/#{filename}")
    content = object.content(reload=true)

    puts "Backup file:"
    puts "  name: #{filename}"
    puts "  type: #{object.content_type}"
    puts "  size: #{content.size} bytes"
    puts "\n"

    return content
  end

  def HerokuMongoBackup::remove_old_backup_files(bucket, files_number_to_leave)
    excess = ( object_keys = bucket.objects.find_all(:prefix => "backups/").map { |o| o.key }.sort ).count - files_number_to_leave
    (0..excess-1).each { |i| bucket.objects.find(object_keys[i]).destroy } if excess > 0
  end

when 'aws/s3'
  def HerokuMongoBackup::s3_connect(args)
    service = AWS::S3.new(args.slice(:access_key_id, :secret_access_key))
    service.buckets[args[:bucket_name]]
  end

  def HerokuMongoBackup::s3_list(directory)
    directory.objects
  end

  def HerokuMongoBackup::s3_upload(bucket, filename)
    AWS::S3::S3Object.store("backups/#{filename}", open(filename), bucket)
  end

  def HerokuMongoBackup::s3_download(bucket, filename)
    AWS::S3::S3Object.value("backups/#{filename}", bucket)
  end

  def HerokuMongoBackup::remove_old_backup_files(bucket, files_number_to_leave)
    excess = ( object_keys = AWS::S3::Bucket.find(bucket).objects(:prefix => 'backups/').map { |o| o.key }.sort ).count - files_number_to_leave
    (0..excess-1).each { |i| AWS::S3::S3Object.find(object_keys[i], bucket).delete } if excess > 0
  end

when 'fog'
  def HerokuMongoBackup::s3_connect(args)
    connection = Fog::Storage.new({
      :provider                 => 'AWS',
      :aws_access_key_id        => args[:access_key_id],
      :aws_secret_access_key    => args[:secret_access_key],
    })
    connection.directories.new(:key => args[:bucket_name])
  end

  def HerokuMongoBackup::s3_list(directory)
    directory.files
  end

  def HerokuMongoBackup::s3_upload(directory, filename)
    file = open(filename)
    file = directory.files.create(
      :key    => "backups/#{filename}",
      :body   => open(filename),
      :public => true
    )
  end

  def HerokuMongoBackup::s3_download(directory, filename)
    directory.files.get("backups/#{filename}").body
  end

  def HerokuMongoBackup::remove_old_backup_files(directory, files_number_to_leave)
    total_backups = directory.files.all.size
    
    if total_backups > files_number_to_leave
      
      files_to_destroy = (0..total_backups-files_number_to_leave-1).collect{|i| directory.files.all[i] }
      
      files_to_destroy.each do |f|
        f.destroy
      end
    end
  end

else
  logging = Logger.new(STDOUT)
  logging.error "\n\nheroku-mongo-backup: Please include one of #{gems} gem in applications Gemfile for uploading backup to S3 bucket. (ignore this if using FTP)\n\n"
end




