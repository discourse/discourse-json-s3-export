# name: disource-json-s3-exporter
# about: Export your Discourse data to S3 in Json Format
# version: 0.0.1
# authors: Samer Masry <samer.masry@gmail.com>


enabled_site_setting :json_s3_export_enabled

after_initialize do
  require 'active_record/core'
  module ::Jobs
    class JsonS3Export < Jobs::Scheduled
      BATCH_SIZE = 1000
      sidekiq_options queue: 'low'

      every 1.day
      def execute(args)
        return unless SiteSetting.json_s3_export_enabled
        Discourse.enable_readonly_mode

        ActiveRecord::Base.subclasses.each do |ar_class|
          table_name = ar_class.table_name
          begin
            ar_class.all.find_in_batches(batch_size: BATCH_SIZE).with_index do |group, batch|
              Tempfile.open(table_name) do |f|
                Zlib::GzipWriter.open(f) do |gz|
                  group.each do |record|
                    gz.puts record.to_json
                  end
                end
                client = Aws::S3::Client.new(
                  region: SiteSetting.json_s3_export_region,
                  access_key_id: SiteSetting.json_s3_export_access_key,
                  secret_access_key: SiteSetting.json_s3_export_secret_key
                )
                s3 = Aws::S3::Resource.new(client: client)
                file_name = "#{table_name}/data-#{batch}.gz"
                obj = s3.bucket(SiteSetting.json_s3_export_bucket).object(file_name)
                obj.upload_file(f)
              end
            end
          rescue => e
            # A few tables are not able to be uploaded
            # caused by no primary key
            Sidekiq.logger.error "Unable to upload #{ar_class.to_s}"
          end
        end
      ensure
        Discourse.disable_readonly_mode
      end
    end
  end
end
