require 'spec_helper'
require 'state_map'

describe StateMap do
  let(:connection) { StateMap.connection }
  describe '.connection' do
    context 'on first connection' do
      it 'creates a new database file' do
        expect(Dir['./data/test/**/*.pstore']).to be_empty # test of spec_helper
        connection
        expect(Dir['./data/test/**/*.pstore']).to include("./data/test/state_map.pstore")
      end

      it 'seeds the database with required seed data' do
        expect(connection.keys).to contain_exactly(:current_id, :data_store_id_ranges, :starting_id_of_current_data_store)
      end
    end

    context 'on subsequent connections' do
      it 'does not create a new database file' do
        connection
        dbs_after_first_connection = Dir['./data/test/**/*.pstore']
        connection
        expect(Dir['./data/test/**/*.pstore']).to eq(dbs_after_first_connection)
      end
    end
  end

  describe '.increment_current_id' do
    it 'increments and returns the current_id field' do
      before_state = connection.get(:current_id)
      returned_id = StateMap.increment_current_id
      after_state = connection.get(:current_id)
      expect(after_state).to eq(returned_id)
      expect(returned_id).to eq(before_state + 1)
    end
  end

  describe '.data_store_for_new_record' do
    context 'when data_store(s) has already been closed' do
      before do
        allow_any_instance_of(PstoreConnection).to receive(:get)
          .with(:data_store_id_ranges)
          .and_return([0...100, 100...200, 200...300])
      end

      it 'returns a db file path for the not-yet-closed id range' do
        expect(StateMap.data_store_for_new_record(301)).to eq("./data/test/data_store_3.pstore")
      end
    end

    context 'when a data_store has not yet been closed' do
      it 'returns a db file path for _0.pstore' do # default behavior with fresh database
        expect(StateMap.data_store_for_new_record(1)).to eq("./data/test/data_store_0.pstore")
      end
    end

    context 'when it is time to rotate files' do
      before do
        FileUtils.copy('./spec/support/22_byte_file.txt', './data/test/data_store_0.pstore')
        stub_const('StateMap::MAX_DATA_STORE_SIZE', 21)
      end

      it 'closes the current data store and sets the starting_id_of_current_data_store to the id' do
        StateMap.data_store_for_new_record(5)
        expect(connection.get(:data_store_id_ranges)).to eq([0...5])
        expect(connection.get(:starting_id_of_current_data_store)).to eq(5)
      end

      it 'returns a path to a new data store' do
        expect(StateMap.data_store_for_new_record(5)).to eq('./data/test/data_store_1.pstore')
      end
    end
  end

  describe '.find_data_store_by_id' do
    context 'when no data_stores have yet been closed yet' do # ie, default with fresh db
      it 'returns a path to _0.pstore' do
        expect(StateMap.find_data_store_by_id(999999)).to eq("./data/test/data_store_0.pstore")
      end
    end

    context 'when data_store_id_ranges have been set' do
      before do
        allow_any_instance_of(PstoreConnection).to receive(:get)
          .with(:data_store_id_ranges)
          .and_return([0...100, 100...200, 200...300])
      end

      context 'for ids in the non-active pstore' do
        let(:test_id) { 101 }
        it 'returns a path to the appropriate pstore' do
          expect(StateMap.find_data_store_by_id(test_id)).to eq("./data/test/data_store_1.pstore")
        end
      end

      context 'for ids in the active pstore' do
        let(:test_id) { 301 }
        it 'returns a path to the appropriate pstore' do
          expect(StateMap.find_data_store_by_id(test_id)).to eq("./data/test/data_store_3.pstore")
        end
      end
    end
  end

  describe '.map_data_stores_by_ids' do
    let(:test_ids) { [1, 101, 201, 301, 401] }
    context 'when no data_stores have yet been closed yet' do # ie, default with fresh db
      it 'returns a hash with path to _0.pstore as key, all ids as values' do
        expect(StateMap.map_data_stores_by_ids(test_ids)).to eq({"./data/test/data_store_0.pstore" => test_ids})
      end
    end

    context 'when data_store_id_ranges have been set' do
      before do
        allow_any_instance_of(PstoreConnection).to receive(:get)
          .with(:data_store_id_ranges)
          .and_return([0...100, 100...200, 200...300])
      end

      it 'returns a hash with pstore paths as keys and ids as values' do
        expect(StateMap.map_data_stores_by_ids(test_ids)).to eq({
          "./data/test/data_store_0.pstore" => [1],
          "./data/test/data_store_1.pstore" => [101],
          "./data/test/data_store_2.pstore" => [201],
          "./data/test/data_store_3.pstore" => [301, 401]
          })
      end
    end
  end

  describe '.all_data_store_paths' do
    context 'when no data_stores have yet been closed yet' do # ie, default with fresh db
      it 'returns an array containing only path to _0.pstore' do
        expect(StateMap.all_data_store_paths).to eq(["./data/test/data_store_0.pstore"])
      end
    end

    context 'when data_store_id_ranges have been set' do
      before do
        allow_any_instance_of(PstoreConnection).to receive(:get)
          .with(:data_store_id_ranges)
          .and_return([0...100, 100...200, 200...300])
      end

      it 'returns an array containing all pstore paths' do
        expect(StateMap.all_data_store_paths).to eq([
          "./data/test/data_store_0.pstore",
          "./data/test/data_store_1.pstore",
          "./data/test/data_store_2.pstore",
          "./data/test/data_store_3.pstore"
          ])
      end
    end
  end

  describe 'find_uniq_store_from_compound_key' do
    it 'returns a path with the first four digits of the compound key as the file suffix' do
      expect(StateMap.find_uniq_store_from_compound_key("2020-03-09.xxx.xxx")).to eq("./data/test/uniq_store_2020.pstore")
    end
  end
end
