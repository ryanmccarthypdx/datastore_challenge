require 'spec_helper'
require 'indexer'

describe Indexer do
  let(:index_connections) { Indexer.connections }
  let(:sample_row_1) { ["stb1", "the matrix", "warner bros", "2014-04-01", "4.00", "1:30"] }
  let(:sample_row_2) { ["stb1", "Goldfinger", "warner bros", "2000-01-01", "4.00", "1:30"] }

  describe '.connections' do
    it 'returns an array containing a connection to one index file per PSV_HEADERS member, in order' do
      pstore_index_file_names = index_connections.collect do |c|
        c.store.path.scan(/\.\/data\/test\/indices\/(.+)\.pstore/)
      end
      expect(pstore_index_file_names.flatten).to eq(PSV_HEADERS)
    end
  end

  describe '.index_row' do
    it 'adds each value to its index as-expected' do
      Indexer.index_row(id: 1, row: sample_row_1)
      Indexer.index_row(id: 2, row: sample_row_2)
      expect(index_connections[PSV_HEADERS.index("TITLE")].keys).to contain_exactly("the matrix", "Goldfinger")
      expect(index_connections[PSV_HEADERS.index("STB")].keys).to contain_exactly("stb1")
      expect(index_connections[PSV_HEADERS.index("STB")].get("stb1")).to contain_exactly(1, 2)
    end
  end

  describe '.deindex' do
    before do
      Indexer.index_row(id: 1, row: sample_row_1)
      Indexer.index_row(id: 2, row: sample_row_2)
    end

    it 'removes the deindexed id from all values, deleting any keys with empty values' do
      expect(index_connections[PSV_HEADERS.index("STB")].get("stb1")).to contain_exactly(1, 2) # before state
      Indexer.deindex(data: sample_row_1, id: 1)
      expect(index_connections[PSV_HEADERS.index("STB")].get("stb1")).to eq([2])
      expect(index_connections[PSV_HEADERS.index("TITLE")].keys).not_to include("the matrix")
    end
  end
end
