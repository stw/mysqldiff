
require File.join(File.dirname(__FILE__), %w[spec_helper])

describe Mysqldiff do
  
  before do

  end
  
  it "should initialize" do
    db1 = {:name => 'db1'}
    db2 = {:name => 'db2'}
    mysqldiff = MySQLDiff.new(db1, db2)
    mysqldiff.class.to_s.should eql "MySQLDiff"
  end
  
  it "should connect to each database" do
    db1 = {:name => 'db1'}
    db2 = {:name => 'db2'}
    
    Mysql.stub(:connect).with('localhost','root','','db1', 3306).and_return(Mysql)
    Mysql.stub(:connect).with('localhost','root','','db2', 3306).and_return(Mysql)
    Mysql.stub(:get_server_info).and_return("Mock DB")
    Mysql.stub(:get_server_info).and_return("Mock DB")
    
    mysqldiff = MySQLDiff.new(db1, db2)
    
    mysqldiff.connect(db1).should eql Mysql
    mysqldiff.connect(db2).should eql Mysql
  end
  
  it "should raise an error if connection failed" do
    db1 = {:name => 'db1'}
    db2 = {:name => 'db2'}
    
    # mock the error
    e = Mysql::Error.new()
    e.stub(:errno).and_return("Stub Errno")
    e.stub(:error).and_return("Stub Error")
    e.stub(:sqlstate).and_return("Stub sqlstate")
    
    Mysql.stub(:connect).with('localhost','root','','db1', 3306).and_raise(e)
    
    mysqldiff = MySQLDiff.new(db1, db2)
    
    lambda { mysqldiff.connect(db1) }.should raise_error SystemExit

  end
  
  it "should produce mysqldiff output" do
    
  end
  
  
end

