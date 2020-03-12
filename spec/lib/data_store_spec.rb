require 'spec_helper'
require 'data_store'

describe DataStore do
  let(:test_data_store) { DataStore.new(test_path) }
  let(:test_path) { './data/test/data_store_0.pstore' }
  let(:test_id) { 1234 }
  let(:test_row) { {test_key_1: 'test_value_1', test_value_2: 'test_value_2'} }

  describe '.initialize' do
    it 'has no default seed data' do
      expect(test_data_store.connection.keys).to be_empty
    end
  end

  describe '#create_new_record_from_row' do
    let(:expected_values) { ['test_value_1', 'test_value_2'] }

    it 'sets an entry with id as key and the values from the row as values, ordered correctly' do
      test_data_store.create_new_record_from_row(id: test_id, row: test_row)
      expect(test_data_store.connection.get(test_id)).to eq(expected_values)
      expect(test_data_store.connection.get(test_id)).not_to eq(expected_values.reverse)
    end

    it 'indexes the row' do
      expect(Index).to receive(:index_row).with(id: test_id, row: expected_values)
      test_data_store.create_new_record_from_row(id: test_id, row: test_row)
    end
  end

  describe '#delete' do
    let(:test_entry) { ['test_value_1', 'test_value_2'] }
    before do
      test_data_store.connection.set(test_id, test_entry)
      allow(Index).to receive(:deindex_id).with(data: test_entry, id: test_id)
    end

    it 'deletes the id' do
      expect(test_data_store.connection.get(test_id)).to eq(test_entry) # confirm validity of test
      test_data_store.delete(test_id)
      expect(test_data_store.connection.get(test_id)).to be_nil
    end

    it 'deindexes the data' do
      test_data_store.delete(test_id)
      expect(Index).to have_received(:deindex_id).with(data: test_entry, id: test_id)
    end
  end

  context 'class methods' do
    before do
      PstoreConnection.new('./data/test/data_store_0.pstore').set_multiple_in_single_transaction({1 => ['test', 'row', 'values', '1'], 2 => ['test', 'row', 'values', '2']})
      PstoreConnection.new('./data/test/data_store_1.pstore').set_multiple_in_single_transaction({3 => ['test', 'row', 'values', '3'], 4 => ['test', 'row', 'values', '4']})
    end
    describe '.get_bulk' do
      let(:test_data_store_id_map) { {
        './data/test/data_store_0.pstore' => [1,2],
        './data/test/data_store_1.pstore' => [4],
        } }

      it 'looks up and returns the values as an array' do
        expect(DataStore.get_bulk(test_data_store_id_map)).to eq([['test', 'row', 'values', '1'], ['test', 'row', 'values', '2'], ['test', 'row', 'values', '4']])
      end

      context 'if one of the ids param has been deleted (unlikely race condition with an ingester)' do
        before{ PstoreConnection.new('./data/test/data_store_0.pstore').delete(2) }
        it "simply doesn't include that one in the output" do
          expect(DataStore.get_bulk(test_data_store_id_map)).to eq([['test', 'row', 'values', '1'], ['test', 'row', 'values', '4']])
        end
      end
    end

    describe '.get_all' do
      let(:all_paths) { ['./data/test/data_store_0.pstore', './data/test/data_store_1.pstore'] }

      it 'returns all of the data store values in all data stores' do
        expect(DataStore.get_all(all_paths)).to eq([['test', 'row', 'values', '1'], ['test', 'row', 'values', '2'], ['test', 'row', 'values', '3'], ['test', 'row', 'values', '4']])
      end
    end
  end
end
