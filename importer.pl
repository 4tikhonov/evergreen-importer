#!/usr/bin/perl

use WWW::Mechanize;
use JSON -support_by_pp;

use vars qw/$libpath/;
use FindBin qw($Bin);
BEGIN { $libpath="$Bin" };
use lib "$libpath";
use lib "$libpath/../lib";

use lib '/usr/src/Evergreen-ILS-2.0.3/Open-ILS/src/perlmods';

use MARC::Record;
use MARC::File::XML (BinaryEncoding => 'UTF-8');
use MARC::Charset;
use OpenSRF::System;
use OpenILS::Utils::Fieldmapper;
use OpenSRF::Utils::SettingsClient;
use OpenSRF::EX qw/:try/;
use Encode;
use Unicode::Normalize;
use OpenILS::Application::AppUtils;
use OpenILS::Application::Cat::BibCommon;
use DBI;

MARC::Charset->assume_unicode(1);
my %dbconfig = loadconfig("$Bin/config/db.config");
my ($dbname, $dbhost, $dblogin, $dbpassword) = ($dbconfig{dbname}, $dbconfig{dbhost}, $dbconfig{dblogin}, $dbconfig{dbpassword});
my $dbh = DBI->connect("dbi:Pg:dbname=$dbname;host=$dbhost",$dblogin,$dbpassword,{AutoCommit=>1,RaiseError=>1,PrintError=>0});

my ($start_id, $end_id);
my $bootstrap = '/openils/conf/opensrf_core.xml';
$leaderfile = "$Bin/config/leaders.map";

OpenSRF::System->bootstrap_client(config_file => $bootstrap);
Fieldmapper->import(IDL => OpenSRF::Utils::SettingsClient->new->config_value("IDL"));

# must be loaded and initialized after the IDL is parsed
use OpenILS::Utils::CStoreEditor;
OpenILS::Utils::CStoreEditor::init();

# Control parameters
$SAVE = 1;
$DEBUG = 1;

$file = $ARGV[0];
$leader = $ARGV[1];
print "$file\n";
$command = "xls2csv -s 8859-1 -c '|' -d utf8 $file";
$csv = `$command`;
unless ($leader)
{
    $leader = loadleader($file, $leaderfile);
}

# "Call number","100","245a ","245b","245c","260e","260f","260g","300","500","700","710","740"
my @items = split(/\n/, $csv);
$final = $#items;
push(@test, $items[0]);
$true = 1;
for ($k=0; $k<=$final; $k++)
{
   if ($true)
   {
      push(@test, $items[$k]);
   };
}

foreach $item (@test)
{
   print "CSV $item\n" if ($DEBUG);
   $item=~s/^\"|\"$//g;
   $item = decode('iso-8859-1', $item);
   my @fields = split(/\|/, $item);
   $itemlength = length($item);

   my $callnumber = $fields[0];

   if ($callnumber=~/Call/i) 
   {
        for ($i=0; $i<=$#fields; $i++)
 	{
	     my ($field) = $fields[$i];
	     $field=~s/\"//g;
	     $field=~s/^\s+|\s+$//g;
	     my ($subfield);

	     if ($field=~/(\d+)(\D+)/)
	     {
		$field = $1;
		$subfield = $2;
	     }

	     my $outfield = $field;
	     $subfield="a" if (!$subfield && $outfield!~/call/i);
	     $outfield.="|$subfield" if ($subfield);
	     $map{$i} = $outfield;
	     print "$i => $outfield\n" if ($DEBUG);
	}
   }
   else
   {
	# Real data
        my $record = MARC::Record->new();
        ## add the leader to the record
	$showleader = $leader || '00620nam a22      a 4500';
        $record->leader($showleader);
        my $controlfield = MARC::Field->new( '008', '110701s                                d' );
        $record->insert_fields_ordered($controlfield);

	my ($title, $author, $holding, $thisbarcode);
	#for ($i=0; $i<=$#fields; $i++)
	for ($i=$#fields; $i>=0; $i--)
	{
	    my $mapvalue = $fields[$i];
	    my $uname = $map{$i};

	    if ($uname=~/barcode/i)
	    {
		$thisbarcode = $mapvalue; 
	    }
	    else
	    {
	        my ($field, $subfield) = split(/\|/, $map{$i});
	        $mapvalue=~s/^\s*\"\s*|\s*\"\s*$//g;
	        print "$map{$i} => $mapvalue\n" if ($DEBUG);
	        $title = $mapvalue if ($map{$i} eq '245|a');
	        $author = $mapvalue if ($map{$i} eq '700|a');
	        $holding = $mapvalue if ($field=~/call/i); 

	        if ($field && $mapvalue && $field!~/call/i)
	        {
	            print "M $field $mapvalue\n" if ($DEBUG);
                    my $newfield = MARC::Field->new($field,'','',$subfield => $mapvalue);
                    $record->insert_fields_ordered($newfield);
	        }
	    };
	}

	$xml = $record->as_xml_record();
	print "Metadata: $title $author -$holding- Len:$itemlength\n" if ($DEBUG);
	print "Stored: $xml\n" if ($DEBUG);

	if ($SAVE && $itemlength > 10 && $holding)
	{
            my $editor = OpenILS::Utils::CStoreEditor->new(xact=>1);
            my $record = OpenILS::Application::Cat::BibCommon->biblio_record_xml_import($editor, $xml); #, $source, $auto_tcn, 1, 1);
            print "$record\n" if ($DEBUG);
            $status = $editor->commit();
	
	    # Adding holdings
	    if ($status)
	    {
		# Retreive latest inserted ID
		my $thisid = receive_id($dbh, $title, $author);
		print "$thisid $thisbarcode\n" if ($DEBUG);
		# Store new holding for record with ID
		add_holding($dbh, $thisid, $holding, $thisbarcode);
	    }
	}
   }

};

sub add_holding
{
   my ($dbh, $id, $holding, $thisbarcode, $DEBUG) = @_;
   my $barcode;

   return if (!$id || !$holding);
   
   $str_holding = $holding;
   $str_holding=~s/\W/\_/g;

   #insert into asset.call_number (creator, editor, record, owning_lib, label, label_sortkey) values (5, 5, 1446572, 4, 'Brok 5927/4', 'Brok_5927_4');
   $dbh->do("insert into asset.call_number (creator, editor, record, owning_lib, label, label_sortkey) values (1, 1, $id, 4, '$holding', '$str_holding')");

   # Get id
   $sqlquery = "select id from asset.call_number order by id desc limit 1";
   my $sth = $dbh->prepare("$sqlquery");
   $sth->execute();

   my $call_id = $sth->fetchrow_array();

   unless ($thisbarcode)
   {
       $barcode = check_barcode($dbh);

       # barcode=30051002988267
       $barcode+=1;
       while (check_barcode($dbh, $barcode))
       {
	   $barcode++;
       }
   }
   else
   {
	$thisbarcode=~s/\"//g;
	$barcode = "$thisbarcode"."0";
   }

   print "$call_id $barcode\n" if ($DEBUG);
   $barcode=~s/\"|\'//g;
   $dbh->do("insert into asset.copy (circ_lib, creator, call_number, editor, location, loan_duration, fine_level, barcode) values (4, 5, $call_id, 5, 105, 2, 2, $barcode)");

   return $barcode;
}

sub check_barcode
{
   my ($dbh, $barcode) = @_;

   my $sqlquery = "select barcode from asset.copy";
   $sqlquery.=" where barcode='$barcode'" if ($barcode);
   $sqlquery.=" order by id desc limit 100";
   my $sth = $dbh->prepare("$sqlquery");
   $sth->execute();

   while (my $tmpbarcodedb = $sth->fetchrow_array())
   {
	if ($tmpbarcodedb!~/^3005/ && $tmpbarcode=~/^\d+$/)
	{
	    $barcodedb = $tmpbarcodedb unless ($barcodedb); 
	}
   };

   return $barcodedb;
}

sub receive_id
{
    my ($dbh, $title, $author) = @_;
    my $thisid;

    my $sqlquery = "select id, marc from biblio.record_entry order by id desc limit 1";
    my $sth = $dbh->prepare("$sqlquery");
    $sth->execute();

    my $firstid;
    while (my ($id, $marc) = $sth->fetchrow_array())
    {
	print "$id $marc\n";
	$firstid = $id unless ($firstid);
	if ($title!~/\(/)
	{
	   $thisid = $id if ($marc=~/$title/);
	   $thisid = $id if ($marc=~/$author/);
	};
	last if ($thisid);
    };

    $thisid = $firstid unless ($thisid);
    print "[DEBUG] $thisid\n";
    return $thisid;
}

sub loadleader
{
    my ($file, $leaderfile) = @_;
    my ($leader, %leaders);

    open(lfile, $leaderfile);
    @leaders = <lfile>;
    close(lfile);

    foreach $str (@leaders)
    {
	$str=~s/\r|\n//g;
	my ($name, $leader) = split(/\;\;/, $str);
	$leaders{$name} = $leader;
    }

    foreach $name (sort keys %leaders)
    {
	if ($file=~/$name/i)
	{
	    $leader = $leaders{$name};
	}
    }

    return $leader;
}

sub loadconfig
{
    my ($configfile, $DEBUG) = @_;
    my %config;

    open(conf, $configfile);
    while (<conf>)
    {
        my $str = $_;
        $str=~s/\r|\n//g;
        my ($name, $value) = split(/\s*\=\s*/, $str);
        $config{$name} = $value;
    }
    close(conf);

    return %config;
}
