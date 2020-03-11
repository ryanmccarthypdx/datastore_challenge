require_relative 'pstore_connection'

module Indexer
  class << self
    def connections
      indices_dir = ENV['environment'] == 'test' ? "./data/test/indices/" : "./data/indices/"
      # The following assumes *all* fields should be queryable
      PSV_HEADERS.map do |c|
        PstoreConnection.new(indices_dir + c + ".pstore")
      end
    end

    def index_row(id:, row:)
      connections.zip(row).each do |connection, row_value|
        connection.shovel(row_value, id)
      end
    end

    def deindex(data:, id:)
      connections.zip(data).each do |connection, row_value|
        connection.delete_single_value_from_array(key: row_value, value: id)
      end
    end
  end
end
