#!/usr/bin/ruby -w
#
# Simple script to parse a YAML file to determine the translation required
# in the names and columns from a source to destination database.
#
# It would be great if was possible to provide the column names and their
# respective values as arrays for including in the prepared statements;
# the code would be a lot cleaner, but instead we have to pick apart the
# columns and values to create an insert statement for every value.
#
# The expected YAML format is as follows:
#
# database:
#   - [source_database, src_db_user, src_db_passwd]
#   - [destination_database, dest_db_user, dest_db_passwd]
# table1:
#   - column1
#   - column2
#   - [source_column, destination_column]
# destination_table:
# source_table:
#   - column1
#   - column2
#
# The script will most likely error if the data is not in that format.
# There are no nods or winks to security, bounds checking or any other
# usage consideration. It's quick and dirty and does the job.
#
# julian rawcliffe/May 2014
#
gem 'psych'
require 'mysql2'
require 'psych'
require "tiny_tds"

begin
  srcdb    = nil
  dstdb    = nil
  target   = nil
  useidins = nil
  sqlins   =  nil # Defined here so that error report can include failed query
  tbl_list = Psych.load_file('dbconv-serverlist.yaml')
  
  tbl_list.each{ |tbl|
    # Assume that a database line will be followed by tow attribute lists
    # specifying the source and destination connection details
    if tbl[0] == 'database' then
      dbs = tbl[1]
      sdb, sus, spw = tbl[1][0]
      ddb, dus, dpw = tbl[1][1]
  
      # Connect to the databases
      #srcdb = DBI.connect("DBI:Mysql:#{sdb}:localhost", "#{sus}", "#{spw}")
      #dstdb = DBI.connect("DBI:Mysql:#{ddb}:localhost", "#{dus}", "#{dpw}")
	  srcdb = Mysql2::Client.new(:host => 'localhost', :username => sus, :password => spw, :database => sdb)
      #client = TinyTds::Client.new(:username => 'sa', :password => 'secret', :host => 'mydb.host.net')
      dstdb = TinyTds::Client.new(:username => dus, :password => dpw, :host => '192.168.1.101', :database => ddb)


    else # we have the name of a table, tbl[1] are the columns
      # If a table has no attributes it will be saved as a target table name that
      # is different from the source. The next table entry will be taken as the
      # source table and the attributes will be the columns to use.
      # Not very nice.
      table = tbl[0]
      target ||= table
    
      # Ugliness to trap a remapping from source to target tables.  
      if tbl[1].nil?
        target = table
        next
      end
      puts "Selecting from source table #{table} into target, #{target}."
  
      # Select is the select query
      # insary are the columns to be inserted
      # valary is an array of values to be inserted
      # valstr contains a tally of the values to be inserted
      select = "SELECT"
      insary = Array.new
      valary = Array.new
      tbl[1].each{ |col|
        if col.kind_of?(Array) then
          src = col[0]
          dst = col[1]
        else
          src = dst = col
        end
        # Try to determine whether we need to include the T_SQL identity insert flag
        # If col is id the set the flag
        if /^id$/.match(src) then
          useidins = 1
        end
        select = select +  " #{src},"
        insary << "["+dst+"]"
      }
      # Remove the last ',' from the SELECT and add the table name
      select.gsub!(/,$/, " FROM #{table}")
      
      # Extract the source data and insert it into the destination table
      #stha = srcdb.prepare(select)
      #stha.execute
      stha = srcdb.query(select)
  
      # Clear the destination table before inserting anything
      puts "Deleting old values from the target, #{target}."
      dstdb.execute("DELETE FROM [#{target}]")
      if useidins then
        dstdb.execute("SET IDENTITY_INSERT #{target} ON")
      end
  
      # Grab each row from the source and build up an array of  destination values
      if stha.any? then
        puts "Transferring data for #{target}."
        stha.each do |row|
          row.each do |column|
            c = column[1].to_s
            # How to avoid replacing apostrophes with a particular regex interpolation character
            # https://www.ruby-forum.com/topic/179618. Using gsub!(/'/, "\\'") fails
            # When destination is MySQL or Mariuadb
            #c.gsub!(/'/, "\\\\'")
            # Transact-SQL needs to be handled differently for single and double quotes
            c.gsub!(/'/, "''")
            c.gsub!(/"/, "''")
            valary << "\"#{c}\""
          end
          insstr = insary.join(",")
          valstr = valary.join(",")

          # Insert the data
          sqlins = "INSERT INTO [#{target}](#{insstr}) VALUES(#{valstr})"
          dstdb.execute(sqlins)
          valary.clear
        end
      end
      if useidins then
        dstdb.execute("SET IDENTITY_INSERT #{target} OFF")
      end
      insary.clear
      useidins = nil
    end
    # Clear the target table so that a potential override can be detected
    target = nil
  }
  rescue Mysql2::Error => e
    puts "A MySQL error occurred"
    puts "Error code: #{e.error_number}"
    puts "Error message: #{e.message}"
  rescue TinyTds::Error => e
    puts "An error occurred"
    puts "Error code: #{e.db_error_number}"
    puts "Error message: #{e.message}"
    puts "Last query: #{sqlins}"
  ensure # Stuff that must be done at the end of the script
    # disconnect from database
    srcdb.close if srcdb
    dstdb.close if dstdb
end