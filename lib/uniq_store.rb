require_relative 'pstore_connection'
require_relative 'state_map'

# knows about uniq constraint shape
module UniqStore
  UNIQ_FIELDS = ["DATE", "STB", "TITLE"]
  class << self
    def find_from_row(row)
    end

    def upsert(row:, new_id:)
    end
  end
end
