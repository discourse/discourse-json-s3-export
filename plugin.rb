# name: disource-json-s3-exporter
# about: Export your Discourse data to S3 in Json Format
# version: 0.0.1
# authors: Samer Masry <samer.masry@gmail.com>


enabled_site_setting :json_s3_export_enabled

after_initialize do
  require_dependency 'jobs/base'

  module ::Jobs
    class ExportTableToS3 < Jobs::Base
      BATCH_SIZE = 1000.freeze
      BLACK_LISTED_COLUMNS = {
        'users' => [
          :encrypted_password, :reset_password_token,
          :old_password, :password_salt
        ]
      }.freeze

      sidekiq_options queue: 'low'

      def execute(args)
        ar_class = args[:class_name].constantize
        start = args[:start] || 1
        table_name = ar_class.table_name

        if SiteSetting.json_s3_clear_files_before_upload && start == 1
          delete_existing_files!(table_name)
        end

        ar_class.connection.transaction do
          ar_class.connection.execute('SET TRANSACTION ISOLATION LEVEL SERIALIZABLE')
          ar_class.all.find_in_batches(start: start, batch_size: BATCH_SIZE).with_index do |group, batch|
            Tempfile.open(table_name) do |f|

              Zlib::GzipWriter.open(f) do |gz|
                group.each do |record|
                  gz.puts record.attributes.except(*BLACK_LISTED_COLUMNS[table_name]).to_json
                end
              end

              # Upload File to S3
              upload_to_s3("#{table_name}/data-#{start}.gz", f)

              # schedule next set
              if group.size == BATCH_SIZE
                Jobs.enqueue(:export_table_to_s3, class_name: ar_class.to_s,
                                                  start: group.last[ar_class.primary_key] + 1)
              end
            end
          end
        end
      end

      private

      def upload_to_s3(file_name, data)
        obj = s3_bucket.object(file_name)
        obj.upload_file(data, server_side_encryption: 'AES256')
      end

      def aws_client
        Aws::S3::Client.new(
          region: SiteSetting.json_s3_export_region,
          access_key_id: SiteSetting.json_s3_export_access_key,
          secret_access_key: SiteSetting.json_s3_export_secret_key
        )
      end

      def s3_bucket
        s3 = Aws::S3::Resource.new(client: aws_client)
        s3.bucket(SiteSetting.json_s3_export_bucket)
      end

      def delete_existing_files!(prefix)
        s3_bucket.objects.with_prefix(prefix).delete_all
      end
    end
  end


  module ::Jobs
    class JsonS3Export < Jobs::Scheduled

      sidekiq_options queue: 'low'

      CLASSES_FOR_EXPORT = [
        BadgeGrouping, BadgeType, Badge, Category, CategoryFeaturedTopic,
        CategoryFeaturedUser, CategoryGroup, CategoryTagGroup, CategoryTag,
        CategoryUser, GroupMention, GroupUser, Group, PostActionType,
        PostDetail, Post, TagGroup, Tag, TagUser, TopTopic, TopicTag,
        TopicUser, Topic, UserAction, UserBadge, UserVisit, User
      ].freeze

      every 1.day
      def execute(args)
        return unless SiteSetting.json_s3_export_enabled

        CLASSES_FOR_EXPORT.each do |ar_class|
          Jobs.enqueue(:export_table_to_s3, class_name: ar_class.to_s)
        end
      end
    end
  end
end
