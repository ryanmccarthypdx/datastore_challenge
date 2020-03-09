require 'spec_helper'
require 'uniq_store'
require 'CSV'

describe UniqStore do
  let(:connection) { UniqStore.connection(test_path) }
  let(:test_path) { './data/test/uniq_store_2020.pstore' }
  let(:expected_key) { "2020-01-01.stb04.The Shining" }
  let(:test_row_as_hash) { {
      "STB" => "stb04",
      "TITLE" => "The Shining",
      "PROVIDER" => "Warner Bros",
      "DATE" => "2020-01-01",
      "REV" => "4.0",
      "VIEW_TIME" => "1:30"
    } }
  let(:test_row_as_csv_row) { CSV::Row.new(test_row_as_hash.keys, test_row_as_hash.values) }

  describe '.connection' do
    it 'has no default seed data' do
      expect(connection.keys).to be_empty
    end
  end

  describe '.find_from_row' do
    context 'when the row satisfies uniqueness criteria' do # default behavior with clean db
      it 'returns nil' do
        expect(UniqStore.find_from_row(test_row_as_csv_row)).to be_nil
      end
    end

    context 'when the row violates uniqueness criteria' do
      let(:test_dupe_id) { 12345 }
      before do
        connection.set(expected_key, test_dupe_id)
      end

      it 'returns the id of the data_record to be replaced' do
        expect(UniqStore.find_from_row(test_row_as_csv_row)).to eq(test_dupe_id)
      end
    end
  end

  describe '.upsert' do
    let(:test_new_id)  { 54321 }

    context 'when the record does not exist' do # default on clean dbs
      it 'inserts the record' do
        expect(connection.get(expected_key)).to be_nil
        UniqStore.upsert(row: test_row_as_csv_row, new_id: test_new_id)
        expect(connection.get(expected_key)).to eq(test_new_id)
      end
    end

    context 'when the record already exists' do
      let(:test_dupe_id) { 12345 }
      before do
        connection.set(expected_key, test_dupe_id)
      end

      it 'overwrites the value' do
        expect(connection.get(expected_key)).to eq(test_dupe_id)
        UniqStore.upsert(row: test_row_as_csv_row, new_id: test_new_id)
        expect(connection.get(expected_key)).to eq(test_new_id)
      end
    end
  end
end
