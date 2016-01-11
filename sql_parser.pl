#!/usr/bin/env perl
#########################################################################
# @File Name:    sql_parser.pl
# @Author:       teikay
# @Mail:         teikay@foxmail.com
# @Created Time: 一, 01/11/2016, 19时25分10秒
# @Copyright:    GPL 1.0
# @Description:  
#########################################################################

use strict;
use File::Find;

my $line = 0;
my $packagename = "";
my $procedure = "";
my %tablemap = ();
my %varmap = ();
my $table = "";
my $view = "";

my $path = shift;
while($path eq "-d"){
    my $ddlFile = shift;
    parseTable($ddlFile);
    $path = shift;
}
print "CRUD\tTABLENAME\tALIAS\tCOLUMN\tPACKAGE/PROCEDURE\tFUNCTION\tSOURCE\tLINE\n";

find(\&checkfile, $path);


sub parseTable{
    my ($ddlFile) = @_;
    my $table = "";
    my $view = "";
    my %columns ;
    my (%tabletemp) = ();
    my $column;

    unless(-e $ddlFile){
        print "DDL file is not exists!";
        return;
    }

    eval {
        open IN, "<$ddlFile" or die "can't open file $ddlFile";
    };
    if($@){
        print "$ddlFile read error!";
        return;
    }

    $line = 0;
    while(<IN>){
		$line++;
		if(/CREATE TABLE \"\w+\".\"(\w+)\"/){
			$table = uc($1);
			%columns = ();
		}
		elsif(/CREATE TABLE\s*(\w+)\s*\(/){
			$table = uc($1);
			%columns = ();
		}
		elsif(/ALTER TABLE\s*(\w+)\s*add\s*\(/){
			$table = uc($1);
			%columns = %{$tablemap{$table}};
		}
		elsif(/\"?(\w+)\"? (VARCHAR2|TIMESTAMP|CHAR|NUMBER|DATE)/i){
			if($table ne ""){
				$column = uc($1);
				$columns{$column} = 1;
			}
		}
		elsif(/;\s+$/){
			if($table ne ""){
				$tablemap{$table} = {%columns};
				$table = "";
				%columns = ();
			}
		}
		elsif(/CREATE\s*(FORCE\s*)?VIEW \"\w+\"\.\"(\w+)\"\s*\((.*)\)\s*AS\s*$/){
			$view = $2;
			my $columns = $3;
			my @columns = split(",", $columns);
			%columns = ();
			for $column (@columns){
				if($column =~ /^\s*\"(\w+)\"\s*$/){
					$columns{$1} = 1;
				}
				elsif($column =~ /^(\w+)\s*$/){
					$columns{$1} = 1;
				}
			}
			$tablemap{$view} = {%columns};
			$view = "";
			%columns = ();
		}
		elsif(/CREATE\s*(FORCE\s*)?VIEW\s*\"\w+\"\.\"(\w+)\"\s*\(/){
			$view = $2;
			my $columnstring = $';
			$columnstring =~ s/\r\n//g;
			while(<IN>){
				if(/\)\s*AS\s*$/){
					$columnstring .= $`;
					last;
				}
				$columnstring .= $_;
				$columnstring =~ s/\r\n//g;
			}
			my @list = split(",", $columnstring);
			%columns = ();
			for $column (@list){
				$_ = $column;
				if(/^\s*\"(\w+)\"\s*$/){
					my $name = $1;
					$columns{$name} = 1;
				}
				elsif(/^\s*(\w+)\s*$/){
					my $name = $1;
					$columns{$name} = 1;
				}
				else{
					print "E>$column\n";
				}
			}
			$tablemap{$view} = {%columns};
			$view = "";
			%columns = ();
		}
		elsif(/,\s+$/){
			#print ">> $ddl:$line\t$_";
		}
    }
}

sub checkfile {
    my $filePath = $_;
    my $remain = "";
	my $comment = "0";
	my $case = 0;
	
    eval {
        open IN, "<$filePath" or die "can't open file $filePath";
    };
    if($@){
        print "$filePath read error!";
        return;
    }
    $line = 0;
    while(<IN>){
    	$line++;
		if(/^\s*(--.*)?$/){
		    next;
		}
		
		#remove comment or space after line
		if(/\s*(--.*)?$/){
		    $_ = $`;
		}
		if(/\/\*.*\*\/$/){
		    $_ = "$`$'";
		}
		
		#block comment
		if($comment==0){
			if(/\/\*/){
				$remain .= $`;
				$comment = 1;
				next;
			}
		}
		else{
			if(/\*\//){
				$_ = "$remain$'";
				$remain = "";
				$comment = 0;
			}
			else{
				next;
			}
		}
		
		if(/CREATE\s+(OR\s+REPLACE\s+)?PACKAGE\s+BODY\s+(\w+)/i){
		    $packagename = $2;
		}
		elsif(/CREATE\s+(OR\s+REPLACE\s+)?PROCEDURE\s+(\w+)/i){
		    $procedure = $2;
		    %varmap = ();
		}
		elsif(/(FUNCTION|PROCEDURE)\s+(\w+)/i){
		    $procedure = $2;
		    %varmap = ();
		}

		if(/CASE/){
			my $p = $`;
			if(!($p =~ /^\s*$/) || $remain ne ""){
				print STDERR "enter CASE block ($line)\n";
				$case++;
			}
			else{
				print STDERR "statement CASE ($line)\n";
			    $remain .= $_;
			}
		}
		elsif($case && /END/){
			$case--;
		    print STDERR "end CASE block ($line)\n";
		}

		if(!$case && /(BEGIN|THEN|ELSE|EXCEPTION|IS)\s*$/){
			print STDERR "reset statement ($line)\n";
		    $remain = "";
		    #print "----\n";
		}
		elsif(/;\s*$/){
		    $remain .= "$`;";
		    checksql($remain);
		    $remain = "";
		    #print "----\n";
		}
		else{
		    $remain .= $_;
		}
    }
    close(IN);
}

sub checksql {
    my($stmt) = @_;    
    $_ = $stmt;
	print STDERR "--------($line)\n$_\n--------\n";
    if(/(\w+)\s*:=\s*(.*);\s*$/){
    	#print STDERR "]]$_\n";
    	my $var = $1;
    	my $expr = $2;
    	for my $name (keys %varmap){
    		my $expr_pre = $expr;
    		$expr =~ s/$name/$varmap{$name}/;
    		if($expr ne $expr_pre){
	    		print STDERR "$name:($line)\n$expr\n";
	    	}
    	}
    	$expr =~ s/\s*||\s*//;
    	$expr =~ s/'\s*'//;
    	$varmap{$var} = $expr;
    	#print STDERR ">> $expr\n";
    	parsesql($expr);
    }
    elsif(/(EXEC(UTE)?\s+)?(SELECT|INSERT|UPDATE|DELETE)/i){
		parsesql("$3$'");
    }
}

sub parsesql {
    my($sql) = @_;
    $_ = $sql;

	my %usedtable = ();
    my $table = "";
    my $column = "";
    my $alias = "";
    my $type = "";
    my $rtable = "";
    my $remain = "";
    while($_){
		if(/UPDATE\s+(\w+)/i){
		    $table = $1;
		    $usedtable{$table} = $table;
	    	$_ = "$'";
		    $type = "U";
		    print "U\t$table\t\t\t$packagename\t$procedure\t$path\t$line\n";
		}
		elsif(/INSERT\s+INTO\s+(\w+)/i){
		    $table = $1;
		    $usedtable{$table} = $table;
		    $_ = "$'";
		    $type = "C";
		    print "C\t$table\t\t\t$packagename\t$procedure\t$path\t$line\n";
		}
		elsif(/DELETE\s+FROM\s+(\w+)/i){
		    $table = $1;
		    $usedtable{$table} = $table;
		    $_ = "$'";
		    $type = "D";
		    print "D\t$table\t\t\t$packagename\t$procedure\t$path\t$line\n";
		}
		elsif(/(FROM|JOIN)\s+((\w+(\s+(AS\s+)?\w+)?)(\s*,\s*\w+(\s+(AS\s+)?\"?\w+\"?)?)*)/i){
		    my $tables = $2;
		    #print STDERR "[tables]$tables\n";
		    $_ = "$'";
		    my @table = split(",", $tables);
		    for my $t (@table){
				if($t =~ /(\w+)\s*((AS\s+)?(\"?(\w+)\"?))?/){
				    $table = uc($1);
				    #print STDERR "[table]$table\n";
				    $usedtable{$table} = $table;
				    $alias = uc($5);
				    if($alias =~ /WHERE|PARTITION|GROUP/i){
				    	$alias = "";
				    }
				    $usedtable{$alias} = $table;
				    $type = "R";
				    print "R\t$table\t$alias\t\t$packagename\t$procedure\t$path\t$line\n";
				}
		    }
		}
		else{
		    last;
		}
    }
    
    if($type eq ""){
    	return;
    }
    $alias = "";
    $remain = $sql;
    while($remain){
    	#print "]]$remain\n";
    	$_ = $remain;
    	if(/^\"?(\w+)\"?\.\"?(\w+)\"?/){
    		$alias = uc($1);
    		$column = uc($2);
    		$remain = $';
    		if(exists $usedtable{$alias}){
    			$rtable = $usedtable{$alias};
    			if(exists $tablemap{$rtable}){
    				my %columns = %{$tablemap{$rtable}};
    				if(exists $columns{$column}){
    					if($alias eq $rtable){
    						$alias = "";
    					}
    					print "$type\t$rtable\t$alias\t$column\t$packagename\t$procedure\t$path\t$line\n";
    				}
    				else{
    					print STDERR "line $line: bad column: $column\n";
    				}
    			}
				else{
					print STDERR "line $line: table not found in tablemap: $rtable\n";
				}
    		}
			else{
				print "$type\t\t$alias\t$column\t$packagename\t$procedure\t$path\t$line\n";
			}
    	}
    	elsif(/^\"?(\w+)\b\"?/){
    		$column = uc($1);
    		$remain = $';
    		#print "word:$column\n";
    		my $found = 0;
    		for $alias (keys %usedtable){
    			$rtable = $usedtable{$alias};
    			if(exists $tablemap{$rtable}){
					my %columns = %{$tablemap{$rtable}};
					if(exists $columns{$column}){
    					if($alias eq $rtable){
    						$alias = "";
    					}
						print "$type\t$rtable\t$alias\t$column\t$packagename\t$procedure\t$path\t$line\n";
						$found = 1;
						last;
					}
				}
    		}
    		unless($found){
    			print STDERR "line $line: column not found in sql: $column\n";
    		}
    	}
    	elsif(/^([^A-Za-z0-9_]+)/){
    		$remain = $';
    		#print "symbol: $1\n";
    	}
    	else{
    		#print "abort: $remain\n";
    		last;
    	}
    }
}	
