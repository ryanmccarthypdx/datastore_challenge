require_relative 'pstore_connection'

module StateMap # Small db, will only ever be one for a very long time; knows where everything is
  MAX_DATA_STORE_SIZE = 10000000 # bytes

  class << self
    def connection
      PstoreConnection.new('./data/state_map.pstore', {
                                                        current_id: 0,
                                                        data_store_id_ranges: [],
                                                        starting_id_of_current_data_store: 0
                                                      })
    end

    def increment_current_id
      connection.increment(:current_id)
    end

    def data_store_for_new_record(id_for_new_record)
      data_store_id_ranges = connection.get(:data_store_id_ranges)
      data_store_path = convert_ranges_index_to_data_store_path(data_store_id_ranges.count)
      existing_file_size = File.size(data_store_path) rescue 0 # file doesn't exist on init
      if existing_file_size > MAX_DATA_STORE_SIZE
        data_store_id_ranges.push(connection.get(:starting_id_of_current_data_store)...id_for_new_record)
        connection.set_multiple_in_single_transaction({
          data_store_id_ranges: data_store_id_ranges,
          starting_id_of_current_data_store: id_for_new_record
          })
        data_store_path = convert_ranges_index_to_data_store_path(data_store_id_ranges.count)
      end
      data_store_path
    end

    def find_data_store_by_id(id)
      data_store_id_ranges = connection.get(:data_store_id_ranges)
      index = data_store_id_ranges.index{|r| r.include?(id)} || data_store_id_ranges.count
      convert_ranges_index_to_data_store_path(index)
    end

    def map_data_stores_by_ids(ids)
      data_store_id_ranges = connection.get(:data_store_id_ranges)
      map = {}
      data_store_id_ranges.each_with_index do |range, index|
        matching_ids = ids.select{|id| range.include?(id)}
        map[index] = matching_ids
        ids -= matching_ids
      end
      map[data_store_id_ranges.count] = ids
      map.transform_keys{|k| convert_ranges_index_to_data_store_path(k)}
    end

    def find_uniq_store_from_compound_key(compound_key)
      # Unlike data store which can be built up serially to MAX_DATA_STORE_SIZE,
      # the decision about how to break up the uniq_store files needs to be
      # based on a statistical analysis of the data.
      if ENV['environment'] == 'test'
        "./data/test/uniq_store_#{compound_key[0..3]}.pstore"
      else
        "./data/uniq_store_#{compound_key[0..3]}.pstore"
      end
    end

    def all_data_store_paths
      paths = [convert_ranges_index_to_data_store_path(0)]
      connection.get(:data_store_id_ranges).each_index do |i|
        paths << convert_ranges_index_to_data_store_path(i + 1)
      end
      paths
    end

    private

    def convert_ranges_index_to_data_store_path(index)
      if ENV['environment'] == 'test'
        "./data/test/data_store_#{index}.pstore"
      else
        "./data/data_store_#{index}.pstore"
      end
    end
  end
end
