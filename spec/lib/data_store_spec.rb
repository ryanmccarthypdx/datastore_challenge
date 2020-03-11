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
end
