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

require 'psych'
require "dbi"

begin
  srcdb = nil
  dstdb = nil
  target = nil
  tbl_list = Psych.load_file('db-remapper.yaml')
  
  tbl_list.each{ |tbl|
    # Assume that a database line will be followed by tow attribute lists
    # specifying the source and destination connection details
    if tbl[0] == 'database' then
      dbs = tbl[1]
      s = dbs[0]
      d = dbs[1]
      sdb, sus, spw = tbl[1][0]
      ddb, dus, dpw = tbl[1][1]
  
      # Connect to the databases
      srcdb = DBI.connect("DBI:Mysql:#{sdb}:localhost", "#{sus}", "#{spw}")
      dstdb = DBI.connect("DBI:Mysql:#{ddb}:localhost", "#{dus}", "#{dpw}")
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
        select = select +  " #{src},"
        insary << dst
      }
      # Remove the last ',' from the SELECT and add the table name
      select.gsub!(/,$/, " FROM #{table}")
      
      # Extract the source data and insert it into the destination table
      stha = srcdb.prepare(select)
      stha.execute
  
      # Clear the destination table before inserting anything
      dstdb.execute("DELETE FROM #{target}")
  
      # Grab each row from the source and build up an array of  destination values
      if stha.column_names.size then
        stha.each do |row|
          row.each do |column|
            c = column.to_s
            # How to avoid replacing apostrophes with a particular regex interpolation character
            # https://www.ruby-forum.com/topic/179618. Using gsub!(/'/, "\\'") fails
            c.gsub!(/'/, "\\\\'")
            valary << "'#{c}'"
          end
          insstr = insary.join(",")
          valstr = valary.join(",")
          valstr.gsub!(/\\/, "\\\\")

          # Insert the data
          dstdb.execute("INSERT INTO #{target}(#{insstr}) VALUES(#{valstr})")
          valary.clear
        end
      end
      insary.clear
    end
    # Clear the target table so that a potential override can be detected
    target = nil
  }
  rescue DBI::DatabaseError => e
    puts "An error occurred"
    puts "Error code: #{e.err}"
    puts "Error message: #{e.errstr}"
  ensure # Stuff that must be done at the end of the script
    # disconnect from database
    srcdb.disconnect if srcdb
    dstdb.disconnect if dstdb
end
