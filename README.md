#### Getting started:
Make sure to have ruby installed.  I wrote the project using Ruby 2.6.5.  I like to manage my ruby versions with rbenv: https://github.com/rbenv/rbenv
Be sure to install bundler via: `gem install bundler`
Install the loaded gems by running `bundle`
I recommend--as with all Ruby projects--that the best way to start to understand the code is to run the specs first in documenation mode.  This can be accomplished by running `bundle exec rspec spec -fd`

#### Using the tool:
To start ingestion, run `ruby ingest.rb ./path/to/a/pipe/separated/file`
You can run queries like so: `ruby query.rb -s TITLE,REV,DATE -f DATE=2014-04-01`

If you pass multiple -s or -o flags, it will overwrite.  However, multiple -f flags are additive.

#### My approach
The three things I wanted to optimize for were read performance over write performance, data integrity, and ability to theoretically scale horizontally (though in this case it is writing to local disk, I could imagine a way to use an external disk at some point).  I also wanted to try to use the ruby standard library as much as possible, so I used PStore to interface with the disk.

Because the ingest process requires many many disk operations to complete, and because PStore is so inefficient, ingest is extremely slow.  Query time, however, is much faster.  There are probably a lot of ways to make it faster still.

Regarding data integrity, on ingest all data is indexed for all fields, and the code enforces the uniqueness constraint by hitting the on-disk uniqueness table for each and every row.  Obviously not efficient, but safe.

My approach for dealing with memory limits was to limit the size of the individual data_store files (which hold the entire records) via StateMap::MAX_DATA_STORE_SIZE constant.  I chose an arbitrary limit that seemed more-or-less reasonable, but can be configurable if this was a production system.  The uniq_store is the other likely big table (which enforces the uniqueness constraints).  I split it into tables via year, though this splitting could be expanded to year-and-month quite easily.  I decided that the indices are likely to remain below such a limit (there are only so many unique dates and movies in existence), though likely STB would be the first to have problems.  The rotation system used for data_store could easily be abstracted out and ported over to this one index if necessary.
