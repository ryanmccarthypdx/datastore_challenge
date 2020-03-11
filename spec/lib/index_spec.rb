require 'spec_helper'
require 'index'

describe Index do
  let(:connections_for_ingest) { Index.connections_for_ingest }
  let(:sample_row_1) { ["stb1", "the matrix", "warner bros", "2014-04-01", "4.00", "1:30"] }
  let(:sample_row_2) { ["stb1", "Goldfinger", "warner bros", "2000-01-01", "4.00", "1:30"] }

  describe '.connections_for_ingest' do
    it 'returns an array containing a connection to one index file per PSV_HEADERS member, in order' do
      pstore_index_file_names = connections_for_ingest.collect do |c|
        c.store.path.scan(/\.\/data\/test\/indices\/(.+)\.pstore/)
      end
      expect(pstore_index_file_names.flatten).to eq(PSV_HEADERS)
    end
  end

  describe '.index_row' do
    it 'adds each value to its index as-expected' do
      Index.index_row(id: 1, row: sample_row_1)
      Index.index_row(id: 2, row: sample_row_2)
      expect(connections_for_ingest[PSV_HEADERS.index("TITLE")].keys).to contain_exactly("the matrix", "Goldfinger")
      expect(connections_for_ingest[PSV_HEADERS.index("STB")].keys).to contain_exactly("stb1")
      expect(connections_for_ingest[PSV_HEADERS.index("STB")].get("stb1")).to contain_exactly(1, 2)
    end
  end

  describe '.deindex_id' do
    before do
      Index.index_row(id: 1, row: sample_row_1)
      Index.index_row(id: 2, row: sample_row_2)
    end

    it 'removes the deindexed id from all values, deleting any keys with empty values' do
      expect(connections_for_ingest[PSV_HEADERS.index("STB")].get("stb1")).to contain_exactly(1, 2) # before state
      Index.deindex_id(data: sample_row_1, id: 1)
      expect(connections_for_ingest[PSV_HEADERS.index("STB")].get("stb1")).to eq([2])
      expect(connections_for_ingest[PSV_HEADERS.index("TITLE")].keys).not_to include("the matrix")
    end
  end

  describe '.fetch_ids' do
    before do
      Index.index_row(id: 1, row: sample_row_1)
      Index.index_row(id: 2, row: sample_row_2)
    end

    context 'for an existing index_key' do
      it 'returns an array of the ids' do
        expect(Index.fetch_ids(column_name: "STB", index_key: "stb1")).to eq([1,2])
      end
    end

    context 'for an index_key that cannot be found' do
      it 'returns an empty array' do
        expect(Index.fetch_ids(column_name: "STB", index_key: "stb99999")).to eq([])
      end
    end
  end
end
