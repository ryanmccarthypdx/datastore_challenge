require_relative 'pstore_connection'

module Index
  class << self
    def index_row(id:, row:)
      connections_for_ingest.zip(row).each do |connection, row_value|
        connection.shovel(row_value, id)
      end
    end

    def deindex_id(data:, id:)
      connections_for_ingest.zip(data).each do |connection, row_value|
        connection.delete_single_value_from_array(key: row_value, value: id)
      end
    end

    def fetch_ids(column_name:, index_key:)
      connection(build_path(column_name)).get(index_key) || []
    end

    def connections_for_ingest
      # The following assumes *all* fields should be queryable
      PSV_HEADERS.map do |c|
        connection(build_path(c))
      end
    end

    private

    def build_path(column_name)
      indices_dir = ENV['environment'] == 'test' ? "./data/test/indices/" : "./data/indices/"
      indices_dir + column_name + ".pstore"
    end

    def connection(path)
      PstoreConnection.new(path)
    end
  end
end
