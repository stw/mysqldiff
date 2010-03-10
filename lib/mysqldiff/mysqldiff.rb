# 
#  mysqldiff.rb
#  mysqldiff
#  
#  Created by Stephen Walker on 2010-03-09.
#  Copyright 2010 WalkerTek Interactive Marketing. All rights reserved.
# 

require 'rubygems'
require 'mysql'

class MySQLDiff
  
  def initialize(db1, db2)
    @debug      = false
    @columns    = {}
    @db1_output = ""
    @db2_output = ""
    
    if (db1 == db2)
      $stderr.puts "Error: No need to compare a database to itself."
      return
    end
    
    @db1 = db1
    @db2 = db2
  end
  
  def run
    return if ((!@db1)||(!@db2))
    
    date = Time.now
    puts "-- MySQL Diff created with mysqldiff - #{date}"
    puts "-- http://www.walkertek.com"
    puts "-- Steve Walker <swalker@walkertek.com>\n\n"
    
    @db1 = connect(@db1)
    @db2 = connect(@db2)
    
    puts "\n"
    
    compare_schema
    
    puts "use db1;\n\n" if @db1_output != ""
    puts @db1_output
    puts "use db2;\n\n" if @db2_output != ""
    puts @db2_output
    
    puts "-- Databases Synchronized." if ((@db1_output == "")&&(@db2_output == ""))
    puts "-- Done."
  end
  
  # ========================================================
  # = Connect to database, error out if we have any issues =
  # ========================================================
  def connect(args)
    begin
      # sensible? defaults
      args[:host]     ||= 'localhost'
      args[:port]     ||= 3306
      args[:user]     ||= 'root'
      args[:password] ||= ''
      
      db = Mysql.connect(args[:host], args[:user], args[:password], args[:name], args[:port])
      @debug and puts "-- #{args[:host]} #{args[:name]} server version: " + db.get_server_info
      return db
    rescue Mysql::Error => e
      $stderr.puts "Error code: #{e.errno}"
      $stderr.puts "Error message: #{e.error}"
      $stderr.puts "Error SQLSTATE: #{e.sqlstate}" if e.respond_to?("sqlstate")
      exit
    end
  end
  
  
  # =======================================================================
  # = We should use these if we are doing a real time update to a live db =
  # =======================================================================
  def lock_table(db, table)
    begin
      db.query("LOCK #{table}")
    rescue Mysql::Error => e
      $stderr.puts "Error code: #{e.errno}"
      $stderr.puts "Error message: #{e.error}"
      $stderr.puts "Error SQLSTATE: #{e.sqlstate}" if e.respond_to?("sqlstate")
      exit
    end
  end
  
  def unlock_table(db, table)
    begin
      db.query("UNLOCK #{table}")
    rescue Mysql::Error => e
      $stderr.puts "Error code: #{e.errno}"
      $stderr.puts "Error message: #{e.error}"
      $stderr.puts "Error SQLSTATE: #{e.sqlstate}" if e.respond_to?("sqlstate")
      exit
    end
  end
  
  # ==========================================
  # = Wrap up query with some error handling =
  # ==========================================
  def query(db, query, type = "array")
    begin
      result = db.query(query)
      return (type == "hash" ? result.to_hash : result.to_array)
    rescue Mysql::Error => e
      $stderr.puts "Error code: #{e.errno}"
      $stderr.puts "Error message: #{e.error}"
      $stderr.puts "Error SQLSTATE: #{e.sqlstate}" if e.respond_to?("sqlstate")
      exit
    end
  end
      
  # =================================================================
  # = Compare the schema and output mysql script to update other db =
  # =================================================================    
  def compare_schema
    tables1 = query(@db1, 'show tables')
    tables2 = query(@db2, 'show tables')
    
    # reject tables that are already in this database
    add_to_db2 = tables1.reject {|t| tables2.include?(t) }
    add_to_db1 = tables2.reject {|t| tables1.include?(t) }
    
    # add tables and data for tables not in db
    add_to_db1.each do |table|
      @columns[table] = create_table(@db1_output, @db2, table)
      insert_data(@db1_output, @db2, table)
    end

    add_to_db2.each do |table|
      @columns[table] = create_table(@db2_output, @db1, table)
      insert_data(@db2_output, @db1, table)
    end
    
    # find differences in tables in both dbs 
    in_both = tables1 & tables2
    in_both.each do |table|
      # column differences
      compare_columns(table)
      
      # data differences
      compare_data(table)
    end
  end
  
  # =======================================================================
  # = Compare the table columns and output mysql script to alter other db =
  # =======================================================================
  def compare_columns(table)
    t1_cols = query(@db1, "DESCRIBE #{table}")
    t2_cols = query(@db2, "DESCRIBE #{table}")
    
    # reject cols that already exist in the table
    add_to_t2 = t1_cols.reject {|t| t2_cols.include?(t) }
    add_to_t1 = t2_cols.reject {|t| t1_cols.include?(t) }
    
    changes = false
    add_to_t1.each do |c|
      # find the previous column in the other table so it stays in order
      previous_col = t2_cols.at(t2_cols.index(c) - 1)
      @db1_output << "ALTER TABLE #{table} ADD COLUMN #{c[0]} #{c[1]}" 
      @db1_output << " AFTER #{previous_col[0]};" if previous_col[0]
      @db1_output << " FIRST" if ! previous_col[0]
      @db1_output << "\n"
      changes = true
    end
    @db1_output << "\n" if changes
    
    chnages = false
    add_to_t2.each do |c|
      # find the previous column in the other table so it stays in order
      previous_col = t1_cols.at(t1_cols.index(c) - 1)
      @db2_output << "ALTER TABLE #{table} ADD COLUMN #{c[0]} #{c[1]}" 
      @db2_output << " AFTER #{previous_col[0]};" if previous_col[0]
      @db2_output << " FIRST" if ! previous_col[0]
      @db2_output << "\n"
      changes = true
    end
    @db2_output << "\n" if changes
    
    # TODO - stw - Compare column types and modify if necessary
    
    # store away new column layout for data diff
    @columns[table] = t1_cols | t2_cols
  end
  
  # =================================================================================================
  # = Compare data line by line and add to other db (very slow, maybe won't work for large tables?) =
  # =================================================================================================
  def compare_data(table)
    data1 = query(@db1, "SELECT * FROM #{table}", "hash")
    data2 = query(@db2, "SELECT * FROM #{table}", "hash")
    
    changes = false
    data1.each do |row|
      if ! data2.include?(row)
        to_insert(@db2_output, table, row)
        changes = true
      end
    end  
    @db2_output << "\n" if changes
    
    changes = false
    data2.each do |row|
      if ! data1.include?(row)
        to_insert(@db1_output, table, row)
        changes = true
      end
    end
    @db1_output << "\n" if changes
  end
  
  # ========================================
  # = Insert data for tables from other db =
  # ========================================
  def insert_data(output, db, table)
    result = query(db, "SELECT * FROM #{table}", "hash")
    changes = false
    result.each do |row|
      to_insert(output, table, row)
      changes = true
    end
    output << "\n" if changes
  end
  
  # ====================
  # = Build the insert =
  # ====================
  def to_insert(output, table, row)
    columns = @columns[table].map {|i| i[0] }.join(',')
    values = map_values(row, @columns[table])
    output << "INSERT INTO #{table} (#{columns}) VALUES (#{values});\n"
  end
  
  # ================================================================================
  # = Create the output values as a string, change formatting based on column type =
  # ================================================================================
  def map_values(row, columns)
    values = columns.map do |v|
      # TODO - stw - which other cases do we need to handle?
      case v[1]
        when /int/: 
          row[v[0]] || 'NULL'
        else 
          (row[v[0]].nil? ? 'NULL' : "'" + @db1.escape_string(row[v[0]].to_s) + "'")
      end
    end
    values = values.join(',')     
  end
  
  # ==================================
  # = Create table definition output =
  # ==================================
  def create_table(output, db, table)
    cols = query(db, "DESCRIBE #{table}")
    
    output << "CREATE TABLE #{table} (\n"
    cols.each_with_index do |c, i|
      output << ",\n" if i > 0
      output << "\t#{c[0]} #{c[1]}"
      output << " primary key" if c[3] == "PRI"
      output << " DEFAULT NULL" if c[2] == "YES"
      output << " DEFAULT #{c[4]}" if c[2] == "NO" && c[3] != "PRI"
      output << " #{c[5]}" if c[5] != ""
    end
    output << "\n);\n\n"

    return cols
  end
  
end

# =========================================
# = Add some convenience methods to Mysql =
# =========================================
class Mysql
  class Result
    def to_array
      array = Array.new
      self.each do |i|
        array << i
      end
      return array
    end
    
    def to_hash
      array = Array.new
      self.each_hash do |h|
        array << h
      end
      return array
    end
  end
end