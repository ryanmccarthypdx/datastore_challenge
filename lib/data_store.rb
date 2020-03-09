require_relative 'pstore_connection'

# knows about shape of data store rows
module DataStore
  class << self
    def create_new_record_from_row(id:, row:)
    end

    def delete(id)
    end
  end
end
