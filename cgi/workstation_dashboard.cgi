#!/usr/bin/perl

use strict;
use warnings;
use CGI qw/:standard :cgi-lib/;
use CGI::Carp qw(fatalsToBrowser);
use CGI::Session;
use DBI;
use IO::File;
use POSIX qw(strftime);
use Time::Local;
use XML::Simple;
use JFRC::Utils::DB qw(:all);
use JFRC::Utils::Web qw(:all);
use JFRC::Highcharts qw(:all);

# ****************************************************************************
# * Environment-dependent                                                    *
# ****************************************************************************
# Change this on foreign installation
use constant DATA_PATH => '/opt/informatics/data/';
my $BASE = "/var/www/html/output/";

# ****************************************************************************
# * Constants                                                                *
# ****************************************************************************
(my $PROGRAM = (split('/',$0))[-1]) =~ s/\..*$//;
our $APPLICATION = 'Workstation dashboard';
my @BREADCRUMBS = ('Imagery tools',
                   'http://informatics-prod.int.janelia.org/#imagery');
use constant NBSP => '&nbsp;';

# ****************************************************************************
# * Globals                                                                  *
# ****************************************************************************
# Web
our ($USERID,$USERNAME);
my $Session;
# Database
our $dbh;

# ****************************************************************************
# Session authentication
$Session = &establishSession(css_prefix => $PROGRAM);
&sessionLogout($Session) if (param('logout'));
$USERID = $Session->param('user_id');
$USERNAME = $Session->param('user_name');
my %sth = (
Status => "SELECT value,COUNT(1) FROM entityData WHERE entity_att='Status' GROUP BY 1",
Aging => "SELECT name,e.owner_key,ed.updated_date FROM entity e "
         . "JOIN entityData ed ON (e.id=ed.parent_entity_id) WHERE "
         . "entity_att='Status' AND value='Processing' ORDER BY 3",
);


# ****************************************************************************
# * Main                                                                     *
# ****************************************************************************
&initializeProgram();
&displayDashboard();
# We're done!
if ($dbh) {
  ref($sth{$_}) && $sth{$_}->finish foreach (keys %sth);
  $dbh->disconnect;
}
exit(0);


# ****************************************************************************
# * Subroutines                                                              *
# ****************************************************************************

sub initializeProgram
{
  # Connect to databases
  &dbConnect(\$dbh,'workstation');
  foreach (keys %sth) {
    $sth{$_} = $dbh->prepare($sth{$_}) || &terminateProgram($dbh->errstr)
  }
}


sub displayDashboard
{
  &printHeader();
  # Read status counts from workstation_status.log
  my $file =  DATA_PATH . 'workstation_status.log';
  my $stream = new IO::File $file,"<"
    || &terminateProgram("Can't open $file ($!)");
  my (%chash,%disposition);
  my ($first_date,$last_date) = ('')x2;
  while (defined(my $line = $stream->getline)) {
    chomp($line);
    my($date,$status,$count) = split(/\t/,$line);
    $first_date = $date unless ($first_date);
    if ($last_date ne $date) {
      $last_date = $date;
    }
    $disposition{$status} = ($status =~ /(?:Blocked|Complete|Retired)/)
                            ? 'Complete' : 'In process';
    next if ($status =~ /(?:Blocked|Complete|Retired)/);
    $chash{$date}{$status} = 1*$count;
  }
  $stream->close();
  my (%count,%donut,%piec,%piei);
  my $total = 0;
  $sth{Status}->execute();
  my $ar = $sth{Status}->fetchall_arrayref();
  foreach (@$ar) {
    $count{$_->[0]} = $_->[1];
    $total += $_->[1];
    ($_->[0] =~ /(?:Blocked|Complete|Retired)/) ? $piec{$_->[0]} = $_->[1]
                                                : $piei{$_->[0]} = $_->[1];
    $donut{($_->[0] =~ /(?:Blocked|Complete|Retired)/) ? 'Complete' : 'In process'} += $_->[1];
  }
  my @color = qw(ff6666 6666ff ff66ff 66ffff);
  my $donut1 = &generateHalfDonutChart(hashref => \%donut,
                                       title => 'Disposition',
                                       content => 'disposition',
                                       color => ['50b432','cc6633'],
                                       text_color => 'white',
                                       label_format => "this.point.name",
                                       width => '400px', height => '300px',
                                      );
  my $pie1 = &generateSimplePieChart(hashref => \%piec,
                                     title => 'Completed samples',
                                     content => 'pie1',
                                     color => [qw(4444ff 44ff44 ff9900)],
                                     text_color => '#bbc',
                                     legend => 'right',
                                     width => '400px', height => '300px',
                                    );
  my $pie2 = &generateSimplePieChart(hashref => \%piei,
                                     title => 'Samples in process',
                                     content => 'pie2',
                                     color => \@color,
                                     text_color => '#bbc',
                                     legend => 'right',
                                     width => '400px', height => '300px',
                                    );
  my $chart = &generateSimpleLineChart(hashref => \%chash,
                                       title => 'Sample status history (in process)',
                                       subtitle => "$first_date - $last_date",
                                       content => 'status',
                                       color => \@color,
                                       text_color => '#bbc',
                                       );
  $sth{Aging}->execute();
  $ar = $sth{Aging}->fetchall_arrayref();
  my @delta;
  %piec = ();
  my $now = time;
  foreach (@$ar) {
    my @f = split(/[-: ]/,$_->[-1]);
    $f[1]--;
    my $then = timelocal(reverse @f);
    my $delta_hours = ($now - $then) / 3600;
    if ($delta_hours < 48) {
      $piec{'< 2 days'}++;
    }
    elsif ($delta_hours < 168) {
      $piec{'2 days - 1 week'}++;
    }
    elsif ($delta_hours < 720) {
      $piec{'1 week - 1 month'}++;
    }
    else {
      $piec{'> 1 month'}++;
    }
    $_->[1] =~ s/.+://;
    push @delta,[@$_,sprintf '%.1f',$delta_hours/24];
  }
  my @pcolor;
  push @pcolor,'cc9900' if (exists $piec{'1 week - 1 month'});
  push @pcolor,'cccc33' if (exists $piec{'2 days - 1 week'});
  push @pcolor,'44cc44' if (exists $piec{'< 2 days'});
  push @pcolor,'cc4444' if (exists $piec{'> 1 month'});
  my $pie3 = &generateSimplePieChart(hashref => \%piec,
                                     title => 'Age of Processing samples',
                                     content => 'pie3',
                                     color => \@pcolor,
                                     text_color => '#bbc',
                                     legend => 'right',
                                     width => '600px', height => '400px');
  my $export = &createExportFile(\@delta,'workstation_processing',
                                 ['Sample','User','Start date','Delta days']);
  # Render
  print div({style => 'float: left'},
            div({style => 'float: left'},
                table({id => 'stats',class => 'tablesorter standard'},
                      thead(Tr(th(['Disposition','Status','Count','%']))),
                      tbody(map {Tr(td([$disposition{$_},$_,&commify($count{$_}),
                                        sprintf '%.2f%%',$count{$_}/$total*100]))}
                          sort keys %count)),
                $donut1,br,$pie1,br,$pie2
               ),
            div({style => 'float: left',align => 'center'},$chart,br,$pie3,$export)
           ),
        div({style => 'clear: both;'},NBSP);
  print end_form,&sessionFooter($Session),end_html;
}


sub createExportFile
{
  my($ar,$suffix,$head) = @_;
  my $filename = (strftime "%Y%m%d_%H%M%S",localtime)
                 . "$suffix.xls";
  my $handle = new IO::File $BASE.$filename,'>';
  print $handle join("\t",@$head) . "\n";
  foreach (@$ar) {
    my @l = @$_;
    print $handle join("\t",@l) . "\n";
  }
  $handle->close;
  my $link = a({class => 'btn btn-success btn-xs',
                href => '/output/' . $filename},"Export Processing samples");
  return($link);
}


sub commify
{
  my $text = reverse $_[0];
  $text =~ s/(\d\d\d)(?=\d)(?!\d*\.)/$1,/g;
  return scalar reverse $text;
}


sub printHeader {
  my($onload) = @_;
  my @scripts = map { {-language=>'JavaScript',-src=>"/js/$_.js"} }
                    ('highcharts-4.0.1/highcharts','jquery/jquery.tablesorter',
                     'tablesorter');
  my @styles = map { Link({-rel=>'stylesheet',
                           -type=>'text/css',-href=>"/css/$_.css"}) }
                   qw(tablesorter-jrc1);
  my %load = (load => 'tableInitialize();');
  $load{load} .= " $onload" if ($onload);
  print &standardHeader(title => $APPLICATION,
                        css_prefix => $PROGRAM,
                        script => \@scripts,
                        style => \@styles,
                        breadcrumbs => \@BREADCRUMBS,
                        expires => 'now',
                        %load),
        start_multipart_form;
}
