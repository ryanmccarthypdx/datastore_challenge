require_relative 'data_store'
require_relative 'state_map'
require_relative 'uniq_store'
require_relative '../config/psv_headers'
require 'csv'
require 'pathname'

class Ingester
  attr_accessor :state_map, :file_path

  def initialize(file_path)
    raise("The header row in #{file_path} indicates an unexpected PSV format, aborting!") unless CSV.open(file_path, col_sep: "|", &:readline) == PSV_HEADERS
    @file_path = file_path
    @state_map = StateMap.new
  end

  def ingest(file_path = @file_path)
    CSV.foreach(file_path, headers: true, col_sep: "|") do |row|
      upsert_row(row)
    end
  end

  private

  def upsert_row(row)
    id_to_upsert = state_map.increment_current_id
    data_store_path = state_map.data_store_for_new_record(id_to_upsert)
    DataStore.new(data_store_path).create_new_record_from_row(id: id_to_upsert, row: row)

    if id_to_delete = UniqStore.find_from_row(row)
      delete_data_store_path = state_map.find_data_store_by_id(id_to_delete)
      DataStore.new(delete_data_store_path).delete(id_to_delete)
    end

    UniqStore.upsert(row: row, new_id: id_to_upsert)
  end
end
