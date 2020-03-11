require 'spec_helper'
require 'data_store'

describe DataStore do
  let(:connection) { DataStore.connection(test_path) }
  let(:test_path) { './data/test/data_store_0.pstore' }
  let(:test_id) { 1234 }
  let(:test_row) { {test_key_1: 'test_value_1', test_value_2: 'test_value_2'} }

  describe '.connection' do
    it 'has no default seed data' do
      expect(connection.keys).to be_empty
    end
  end

  describe '.create_new_record_from_row' do
    let(:expected_values) { ['test_value_1', 'test_value_2'] }
    before do
      allow(StateMap).to receive(:data_store_for_new_record).with(test_id).and_return(test_path)
    end

    it 'sets an entry with id as key and the values from the row as values, ordered correctly' do
      DataStore.create_new_record_from_row(id: test_id, row: test_row)
      expect(connection.get(test_id)).to eq(expected_values)
      expect(connection.get(test_id)).not_to eq(expected_values.reverse)
    end

    it 'indexes the row' do
      expect(Index).to receive(:index_row).with(id: test_id, row: expected_values)
      DataStore.create_new_record_from_row(id: test_id, row: test_row)
    end
  end

  describe '.delete' do
    let(:test_entry) { ['test_value_1', 'test_value_2'] }
    before do
      allow(StateMap).to receive(:find_data_store_by_id).with(test_id).and_return(test_path)
      connection.set(test_id, test_entry)
      allow(Index).to receive(:deindex_id).with(data: test_entry, id: test_id)
    end

    it 'deletes the id' do
      expect(connection.get(test_id)).to eq(test_entry) # confirm validity of test
      DataStore.delete(test_id)
      expect(connection.get(test_id)).to be_nil
    end

    it 'deindexes the data' do
      DataStore.delete(test_id)
      expect(Index).to have_received(:deindex_id).with(data: test_entry, id: test_id)
    end
  end

  context 'bulk methods' do
    before do
      PstoreConnection.new('./data/test/data_store_0.pstore').set_multiple_in_single_transaction({1 => ['test', 'row', 'values', '1'], 2 => ['test', 'row', 'values', '2']})
      PstoreConnection.new('./data/test/data_store_1.pstore').set_multiple_in_single_transaction({3 => ['test', 'row', 'values', '3'], 4 => ['test', 'row', 'values', '4']})
    end
    describe '.get_bulk' do
      let(:test_filtered_ids) { [1,2,4] }
      before do
        allow(StateMap).to receive(:map_data_stores_by_ids)
          .with(test_filtered_ids)
          .and_return({
            './data/test/data_store_0.pstore' => [1,2],
            './data/test/data_store_1.pstore' => [4],
            })
      end

      it 'looks up and returns the values as an array' do
        expect(DataStore.get_bulk(test_filtered_ids)).to eq([['test', 'row', 'values', '1'], ['test', 'row', 'values', '2'], ['test', 'row', 'values', '4']])
      end

      context 'if one of the ids param has been deleted (unlikely race condition with an ingester)' do
        before{ PstoreConnection.new('./data/test/data_store_0.pstore').delete(2) }
        it "simply doesn't include that one in the output" do
          expect(DataStore.get_bulk(test_filtered_ids)).to eq([['test', 'row', 'values', '1'], ['test', 'row', 'values', '4']])
        end
      end
    end

    describe '.get_all' do
      before{ allow(StateMap).to receive(:all_data_store_paths).and_return(['./data/test/data_store_0.pstore', './data/test/data_store_1.pstore']) }
      it 'returns all of the data store values in all data stores' do
        expect(DataStore.get_all).to eq([['test', 'row', 'values', '1'], ['test', 'row', 'values', '2'], ['test', 'row', 'values', '3'], ['test', 'row', 'values', '4']])
      end
    end
  end
end
