#!/usr/bin/perl
#
# Pulls customer bandwidth data from market pollnodes using
# our port mappings stored in the bw_policy_mappings
#
#

use IO::Handle;
use DBI;
use POSIX;
use Data::Dumper;
my $DB_USER = "sherpa_rep";
my $DB_PASS = "sherpa#vista";

my $PDB_USER = "jura_rep";
my $PDB_PASS = "flip#counters21";

autoflush STDOUT 1;
autoflush STDERR 1;

# Get the date and time that corresponds to the beginning of the current month
# which will be the default date/time that we start replicating from if there
# is no date already stored in the DAL
#
@ltime = localtime(time());
#my @testPolicies = (311600,1913,303693,306803,307635,1556,311628,303876,311600,311560,306326,311500,311445,311764);
my @testPolicies = (311834);
my @missingPolicies = ();
($second, $minute, $hour, $dayOfMonth, $month, $yearOffset, $dayOfWeek, $dayOfYear, $daylightSavings) = localtime();
$year = 1900 + $yearOffset;
$month++;
my $mday = 1;
my $start_date = sprintf("%4d-%02d-%02d", $year,$month,$mday);
my $start_date = "2007-04-01 00:00:00";

my $orig_start_epoch = 0;

my @rates_in = ();
my @rates_out = ();
my @sample_times = ();
my @epoch_times = ();
$counter = 0;

# Here we fork and set up 4 copies to make things go faster
for($thidx = 0;$thidx < 8;$thidx++) {

     my $childpid = fork();
     if($childpid) {

	my $bv_dbh = DBI->connect("DBI:Pg:host=127.0.0.1;dbname=bayvista",$DB_USER,$DB_PASS,{RaiseError=>0});

	# Prepare our date statement handle
	my $sth_epoch_select = $bv_dbh->prepare("select date_part('epoch', ?::timestamp)");

	# Policy select.
	#
	my $sth = $bv_dbh->prepare("select c.policy_id,c.group_id,c.name, g.roa_id
			   from customer_bandwidth_policies c, groups g
			   where c.group_id=g.group_id and g.roa_id IS NOT NULL and
			   c.active=1 and (c.policy_id % 8 = $thidx) order by policy_id asc");

	my $test_stm="select c.policy_id,c.group_id,c.name, g.roa_id
               from customer_bandwidth_policies c, groups g
               where c.group_id=g.group_id and g.roa_id IS NOT NULL and
               c.active=1 and (c.policy_id % 8 = $thidx) order by policy_id asc";
	#print Dumper($test_stm);
	if(!$sth->execute()) {

	    my $errstr = DBI::errstr;
	    &send_alerts("Alert on jura", "Pollnode replication failure\n$errstr");
	    exit;

	}

	while(my @row = $sth->fetchrow_array) {

	    my $policy_id = $row[0];
	    my $group_id = $row[1];
	    my $name = $row[2];
	    my $roa_id = $row[3];


	    # Get sample_time boundaries
	    my $sth1 = $bv_dbh->prepare("select date_part('epoch', sample_time) from customer_bandwidth_data where
					 policy_id=$policy_id and sample_time>'2010-01-01' order by sample_time desc limit 1");



	   if($sth1->execute()) {
		if(my @srow = $sth1->fetchrow_array) {

		    $start_epoch = $srow[0];

		}
		else {

		    $start_epoch = $orig_start_epoch;

		}
	    }
	    else {

		my $errstr = DBI::errstr;
		print "Could not read sample_time boundaries\n  $errstr\n";
		&send_alerts("Alert on jura", "Pollnode replication failure\n$errstr");
		exit;

	    }


	    $sth1->finish();

	    #print "START  $policy_id  $start_epoch\n";

		 if(grep(/^$policy_id$/,@testPolicies)){
          #print "Policy number $policy_id starter is included $stime\:\:abstime";
          print "Found faulty policy_ip:: $policy_id \n"; exit;
		 # exit;
        }else{
          print "policy_id:: $policy_id \n";
         # print "Policy number $policy_id not included in watch list";
        }


	    # Get list of interfaces from the interface table including the ifIndex, hostname and ip
	    # which we will use to match to pollnode

            my $intf_sth = $bv_dbh->prepare("select m1.instance,m1.alias,m1.host_device,m2.ip_address from
                               interfaces m1,switches m2 where interface_id in
                               (select interface_id from bw_policy_mappings where policy_id=$policy_id and active='t') and
                               m1.host_device=m2.host_name and m2.active='t' group by host_device, instance, alias, ip_address");

            $intf_sth->execute() || die DBI::errstr;

		  my $numRows = $intf_sth->rows;
      	  if($numRows == 0 and index($name,':IP:')> 0){
        	#push @missingPolicies, $policy_id;
			$counter++; print "Counting $counter with policy_id --> $policy_id name --> $name, group_id --> $group_id \n";

			$intf_sth = $bv_dbh->prepare("select m1.instance,m1.alias,m1.host_device,m2.ip_address from
                               interfaces m1,switches m2 where interface_id = $group_id and
                               m1.host_device=m2.host_name and m2.active='t' group by host_device, instance, alias, ip_address;");
            $intf_sth->execute() || die DBI::errstr;
      	   }
		  $numRows = $intf_sth->rows;
           if($numRows == 0 and grep(/^$policy_id$/,@testPolicies)){
              print "Unable to find information for policy $policy_id part of test group \n";
            }elsif($numRows > 0 and grep(/^$policy_id$/,@testPolicies) ){
              print "Information found for policy $policy_id ($numRows) \n";
            }



	    my $union_str = "";
	    my $id = 0;

	    my @ifIndex = ();
	    my @hostname = ();
	    my @ip_address = ();
	    my $ifcount = 0;
	    while(@row2 = $intf_sth->fetchrow_array) {

		push(@ifIndex, $row2[0]);
		push(@hostname, $row2[2]);
		push(@ip_address, $row2[3]);
		$ifcount++;
	    }
	    $intf_sth->finish();

	    my %bytes_in = ();
	    my %bytes_out = ();
	    my @sample_times = ();

	    for($idx = 0;$idx < $ifcount;$idx++) {

		my $name = $hostname[$idx];
# Hack fix
if($name eq "atl1-access-01") {
    $name = "atl-access-01";
}
if($name eq "atl1-access-02") {
    $name = "atl-access-02";
}

		my $address = $ip_address[$idx];
		my $q1 = "select a.pollnode_id,a.table_name,b.ip_address from switches a, pollnodes b
			  where (a.ip_address='$address' or a.host_name='$name')
			  and a.pollnode_id=b.pollnode_id and a.active='t'
        and a.pollnode_id in (select pollnode_id from poller_capabilities where priority=1 and type_id=5)";

    my $q1 = "select c.pollnode_id,a.table_name,c.ip_address from switches a, pollnodes b, pollnodes c
        where (a.ip_address='$address' or a.host_name='$name')
        and a.pollnode_id=b.pollnode_id and a.active='t'
        and a.pollnode_id in (select pollnode_id from poller_capabilities where priority=1 and type_id=5)
        and c.pollnode_id not in (select pollnode_id from poller_capabilities where priority=1 and type_id=5)
        and c.location_abbr=b.location_abbr";

		my $sth2 = $bv_dbh->prepare($q1);
		if($sth2->execute()) {
			my $pollNumRow = $sth2->rows;
			if($pollNumRow==0 and grep(/^$policy_id$/,@testPolicies)){
         		 print " policy number $policy_id with ip address $address and hostname $name returned no pollnode info \n ";
        	}elsif(grep(/^$policy_id$/,@testPolicies)){
         		 print " policy number $policy_id with ip address $address and hostname $name returned($pollNumRow) pollnode info \n ";
        	}
		    if(@trow = $sth2->fetchrow_array) {
			my $pollnode_id = $trow[0];
			my $table_name = $trow[1];
			my $pollnode_address = $trow[2];
# Use the NE server for NAS
if($pollnode_address eq '67.216.160.244') {
    $pollnode_address = '10.226.156.4';
}

# Use the NE server for RIC
if($pollnode_address eq '74.84.192.244') {
    $pollnode_address = '10.227.156.64';
}

# Use the NE server for ATL
#if($pollnode_address eq '10.226.28.64') {
#    $pollnode_address = '65.254.208.244';
#}

# Use the NE server for SFL
if($pollnode_address eq '96.46.240.45') {
    $pollnode_address = '96.46.240.244';
}

# Use the NE server for LOU

if($pollnode_address eq '10.255.10.229') {
    $pollnode_address = '216.26.129.59';
}

			my $pollnode_dbh = DBI->connect("DBI:Pg:host=$pollnode_address;dbname=pollnode",$PDB_USER,$PDB_PASS,
			       {PrintError=>0});

      my $archive_table_name = "NONE";
			if($pollnode_dbh) {
          my $archiveq = "select archive_table,max_stamp from archives where archive_table
                    like '$table_name%' order by max_stamp desc limit 1";

          my $archive_sth = $pollnode_dbh->prepare($archiveq);
          if($archive_sth->execute()) {
			my $archNumRow = $archive_sth->rows;
            if($archNumRow==0 and grep(/^$policy_id$/,@testPolicies)){
              print " policy number $policy_id with pollnode_address $pollnode_address and table like $table_name returned no archive_table info \n ";
            }elsif(grep(/^$policy_id$/,@testPolicies)){
              print " policy number $policy_id with pollnode_address $pollnode_address and table like $table_name returned good($archNumRow) archive_table info \n";
            }

              if( @archiverow = $archive_sth->fetchrow_array) {;
                  $archive_table_name = $archiverow[0];
              }
          }
          $archive_sth->finish();


			    my $maxq = "select sample_time from $table_name where ifindex=$ifIndex[$idx]
					order by sample_time desc limit 1";
			    my $max_sth = $pollnode_dbh->prepare($maxq);
			    my $maxs = '2028-01-10 15:11:00';

			    if($max_sth->execute()) {
				if( @maxrow = $max_sth->fetchrow_array) {;
				    $maxs = $maxrow[0];
				}
			    }
			    $max_sth->finish();
			    my $q2 = "select bytes_in,bytes_out, date_part('epoch', sample_time) from $table_name where
				      date_part('epoch', sample_time) > $start_epoch and ifindex=$ifIndex[$idx] and
				      sample_time < '$maxs'
				      order by sample_time asc";

$archive_table_name = "NONE";
          if($archive_table_name ne "NONE") {
              $q2 = "select bytes_in,bytes_out, date_part('epoch', sample_time),sample_time from $table_name where
                    date_part('epoch', sample_time) > $start_epoch and ifindex=$ifIndex[$idx] and
                    sample_time < '$maxs' union
                    select bytes_in,bytes_out, date_part('epoch', sample_time),sample_time from $archive_table_name where
                    date_part('epoch', sample_time) > $start_epoch and ifindex=$ifIndex[$idx] and
                    sample_time < '$maxs'
                    order by sample_time asc";
          }

			    my $sth3 = $pollnode_dbh->prepare($q2);
			    if($sth3->execute) {

				my $epocNumRow = $sth3->rows;
            	if($epocNumRow==0 and grep(/^$policy_id$/,@testPolicies)){
              		print " policy number $policy_id with archive_table_name == $archive_table_name  and table like $table_name returned no bytes_in, bytes_out info and max=$maxs, start_epoc=$start_epoch and ifindex=$ifIndex[$idx] \n ";
            	}elsif(grep(/^$policy_id$/,@testPolicies)){
              		print " policy number $policy_id with archive_table_name == $archive_table_name  and table like $table_name returned good bytes_in, bytes_out a total of $epocNumRow and max=$maxs, start_epoc=$start_epoch and ifindex=$ifIndex[$idx] \n ";
            	}

				while(@drow = $sth3->fetchrow_array) {
				    my $bin = $drow[0];
				    my $bout = $drow[1];
				    my $stime = $drow[2];
				    if($bytes_in{$stime}) {
					$bytes_in{$stime} += $bin;
					$bytes_out{$stime} += $bout;
				    }
				    else {
					$bytes_in{$stime} = $bin;
					$bytes_out{$stime} = $bout;
				    }

				}
			    }
			    $sth3->finish();
			    $pollnode_dbh->disconnect();
			}
		    }


		 }
		 $sth2->finish();
	    }

	    foreach $stime (sort keys %bytes_in) {
		$bin = $bytes_in{$stime};
		$bout = $bytes_out{$stime};
		if($bin < 0) {
		    $bin = 0;
		}
		if($bout < 0) {
		    $bout = 0;
		}
		if($bin > 6750000000000) {
		    $bin = 0;
		}
		if($bout > 6750000000000) {
		    $bout = 0;
		}
		my $sth4 = $bv_dbh->prepare("insert into customer_bandwidth_data
			  values($policy_id, $stime\:\:abstime, $bin, $bout)");
		#$sth4->execute();

		if(grep(/^$policy_id$/,@testPolicies)){
		  print "Policy number $policy_id and name==>$name  made it here with time==> $stime\:\:abstime and in=$bin, out=$bout \n";
		}else{
 #         print "Policy number $policy_id not included in watch list";
		}
		$sth4->finish();
	    }
#	    $bv_dbh->commit();

#	    $bv_dbh->commit();

	}

	$sth_epoch_select->finish();
	$sth->finish();


	$bv_dbh->disconnect();
        exit(0);

    } # end if block for child process's

} # end for loop over forks
wait();
print "Done with loop";
if (scalar( @missingPolicies) > 0 ){
  $misnum = scalar( @missingPolicies);
  #print "we are missing scaler $misnum the folowing ======================>". join(', ',@missingPolicies);
}else{
  #print "No missing policies";
}

sub send_alerts
{

     my $subject = @_[0];
     my $message = @_[1];

     open (SendMail, "| /usr/sbin/sendmail dev-notices\@peak10.com");
     print SendMail "From: Pollnode monitoring\n";
     print SendMail "To: dev-notices\@peak10.com\n";
     print SendMail "Subject: $subject\n";
     print SendMail "\n";
     print SendMail "\n$message\n";
     close(SendMail);

}
