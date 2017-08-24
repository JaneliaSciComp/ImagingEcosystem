#!/usr/bin/perl

use strict;
use warnings;
use CGI qw/:standard :cgi-lib/;
use CGI::Carp qw(fatalsToBrowser);
use CGI::Session;
use JSON;
use LWP::Simple;
use POSIX qw(ceil);
use Switch;
use JFRC::Highcharts qw(:all);
use JFRC::Utils::Web qw(:all);

# ****************************************************************************
# * Environment-dependent                                                    *
# ****************************************************************************
# Change this on foreign installation
use constant DATA_PATH => '/opt/informatics/data/';
# Should be "All" or "GAL4"
my $DRIVER = 'GAL4';

# ****************************************************************************
# * Constants                                                                *
# ****************************************************************************
(my $PROGRAM = (split('/',$0))[-1]) =~ s/\..*$//;
our $APPLICATION = 'Gen1 ' . (($DRIVER eq 'All') ? '' : $DRIVER . ' ') . 'representative image selector';
my @BREADCRUMBS = ('Imagery tools',
                   'http://informatics-prod.int.janelia.org/#imagery');
my %CONFIG;
use constant NBSP => '&nbsp;';
my $CLEAR = div({style=>'clear:both;'},NBSP);

# ****************************************************************************
# * Globals                                                                  *
# ****************************************************************************
# Web
our ($USERID,$USERNAME);
my $Session;
my $AUTHORIZED = 0;
# General
my %vt_mapping;

# ****************************************************************************
# Session authentication
# ****************************************************************************
$Session = &establishSession(css_prefix => $PROGRAM);
&sessionLogout($Session) if (param('logout'));
$USERID = $Session->param('user_id');
$USERNAME = $Session->param('user_name');
$AUTHORIZED = 1 if (($Session->param('scicomp'))
                    || ($Session->param('workstation_flylight')));
&terminateProgram('You are not authorized to select representative images')
  unless ($AUTHORIZED);


# ****************************************************************************
# * Main                                                                     *
# ****************************************************************************
&initializeProgram();
if (param('plate')) {
  &displayPlate();
}
else {
  displayInput();
}
# We're done!
exit(0);


# ****************************************************************************
# * Subroutines                                                              *
# ****************************************************************************

sub initializeProgram
{
  # Get WS REST config
  my $file = DATA_PATH . 'rest_services.json';
  open SLURP,$file or &terminateProgram("Can't open $file: $!");
  sysread SLURP,my $slurp,-s SLURP;
  close(SLURP);
  my $hr = decode_json $slurp;
  %CONFIG = %$hr;
  $CONFIG{sage}{url} =~ s/flask/flask-dev/; #PLUG
}


sub displayInput
{
  &printHeader("reloadSetup();");
  my $rest = $CONFIG{sage}{url}.'gen1_images?family=dickson&line=BJD_1*';
  my $response = get $rest;
  &terminateProgram("<h3>REST GET returned null response</h3>"
                    . "<br>Request: $rest<br>")
    unless (length($response));
  my $rvar;
  eval {$rvar = decode_json($response)};
  my (%line,%line_count,%pie,%pw);
  my $selected = 0;
  foreach (@{$rvar->{image_data}}) {
    my($line,$driver,$area,$annotator) = @$_{qw(line driver area annotator)};
    next if (($driver eq 'LexA') && ($DRIVER ne 'All'));
    $line{$line}{$driver}{$area} ||= 0;
    $annotator ||= '';
    if ($annotator) {
      $line{$line}{$driver}{$area}++;
      $selected++;
    }
  }
  my (%error,%status);
  my($G,$L) = qw(GAL4_Collection LexA);
  foreach my $l (sort keys %line) {
    my($plate,$well) = $l =~ /BJD_(\d{3})(.{3})/;
    $status{$plate}{$well} = 0;
    if ($line{$l}{$G}{Brain} && $line{$l}{$G}{VNC}) {
      $status{$plate}{$well} = 1;
      $line_count{$l}++;
      $pw{$plate}{$well}{GAL4}++;
      if ($line{$l}{$L}{Brain} && $line{$l}{$L}{VNC}) {
        $line_count{$l}++;
        $pw{$plate}{$well}{LexA}++;
      }
    }
    elsif ($line{$l}{$L}{Brain} && $line{$l}{$L}{VNC}) {
      $status{$plate}{$well} = 1;
      $line_count{$l}++;
      $pw{$plate}{$well}{LexA}++;
    }
    foreach $a (qw(Brain VNC)) {
      foreach my $d ($G,$L) {
        $line{$l}{$d}{$a} ||= 0;
        $error{$plate}{$well}{$d}{$a}++ if ($line{$l}{$d}{$a} > 1);
      }
    }
  }
  # Pie chart
  foreach my $p (keys %pw) {
    foreach my $w (keys %{$pw{$p}}) {
      if ($pw{$p}{$w}{GAL4}) {
        if ($pw{$p}{$w}{LexA}) {
          $pie{'GAL4 and LexA'}++;
        }
        else {
          $pie{'GAL4 only'}++;
        }
      }
      elsif ($pw{$p}{$w}{LexA}) {
        $pie{'LexA only'}++;
      }
    }
  }
  # Get colors
  my %COLOR;
  my $upper = 10;
  my $num_steps = 10;
  my $color_step = 255/$num_steps;
  my($red,$green) = (255,0);
  while ($green < 255) {
    $green += $color_step;
    $green = 255 if ($green > 255);
    $COLOR{$upper} = sprintf "#%02x%02x%02x",($red,$green,0);
    $red -= $color_step;
    $upper += (100/$num_steps);
  }
  $COLOR{0} = '#991900';
  print h1('Plates'),br;
  my($tfw,$ttw) = (0)x2;
  foreach my $p (sort keys %status) {
    my($fw,$tw) = (0)x2;
    foreach (sort keys %{$status{$p}}) {
      $tw++;
      $fw++ if ($status{$p}{$_});
    }
    $tfw += $fw;
    $ttw += $tw;
    my $perc = sprintf '%.2f',$fw/$tw*100;
    my $pround = sprintf '%d',$perc;
    my $color =  $COLOR{&roundup($pround,10)};
    my $plate = div({class => 'plate',
                    style => "background-color: $color"},$p,br,
                    (sprintf "%d/%d (%s%%) wells complete",$fw,$tw,$perc));
    print a({href => "?plate=$p"},$plate);
  }
  my $pie1 = &generateSimplePieChart(hashref => \%pie,
                                     title => 'Drivers',
                                     subtitle => 'Drivers for completed wells',
                                     point_format => '<b>{point.y} wells</b>: '
                                                     .'{point.percentage:.1f}%',
                                     content => 'pie1',
                                     color => ['#44ff44','#4444ff','#ff9900'],
                                     text_color => '#333',
                                     legend => 'right',
                                     width => '400px', height => '300px',
                                    );
  # Overall status
  my $m = sprintf "Found %d images covering %d lines<br>%d image%s selected across %d line%s<br>",
                  scalar(@{$rvar->{image_data}}),scalar(keys %line),
                  $selected,(($selected == 1) ? '' : 's'),
                  scalar(keys %line_count),((scalar(keys %line_count) == 1) ? '' : 's');
  my $perc = sprintf '%.2f',$tfw/$ttw*100;
  $m .= (sprintf "Completed %d/%d wells (%s%%)<br>",$tfw,$ttw,$perc);
  $m = ($DRIVER eq 'All') ? table(Tr(td($m),td($pie1))) : table(Tr(td($m)));
  $m .= h3({align => 'center'},'Progress bar') . br .
        div({class => 'progress'},
            div({class => 'progress-bar progress-bar-info progress-bar-striped',
                 role => 'progressbar', 'aria-valuenow' => $perc,
                 'aria-valuemin' => 0,'aria-valuemax' => 100,
                 style => "width: $perc%"},
                "$perc%"));
  print $CLEAR,br,
        h2({align => 'center'},'Click on a plate to select representative imagery for it.'),
        $CLEAR,hr,
        div({style => 'float: left;'},
            div({style => 'float: left; padding-right: 100px;'},
                "Color key:",
                table({id => 'key'},
                      map {Tr(th({style => 'padding-right: 5px'},
                                 div({style => 'height:100%; width:10; '
                                               . 'background-color:'.$COLOR{$_}},
                                     NBSP)),
                              td(($_) ? (sprintf '%d%% < x <= %d%%',$_-10,$_) : 'x = 0%'))}
                          sort {$a <=> $b} keys %COLOR)),
            div({class => 'boxed',style => 'float: left; min-width: 1000px',},$m)),
        $CLEAR;
  # Plates/wells in error
  if (scalar(keys %error)) {
    $m = h2('Plate/wells in error')
         . 'The following plate/wells have more than one image per driver/area:' . br;
    foreach my $p (sort keys %error) {
      foreach my $w (sort keys %{$error{$p}}) {
        foreach my $d (sort keys %{$error{$p}{$w}}) {
          foreach my $a (sort keys %{$error{$p}{$w}{$d}}) {
            $m .= "$p/$w, $d, $a" . br;
          }
        }
      }
    }
    print br,div({class => "alert alert-danger",role => "alert"},$m);
  }
  print end_form,&sessionFooter($Session),end_html;
}


sub roundup
{
  my $num = shift;
  my $roundto = shift || 1;
  return int(ceil($num/$roundto))*$roundto;
}


sub displayPlate
{
  my($plate) = param('plate');
  &printHeader();
  my $rest = $CONFIG{sage}{url}.'gen1_images?family=dickson&line=BJD_'
             . $plate . '*';
  my $response = get $rest;
  &terminateProgram("<h3>REST GET returned null response</h3>"
                    . "<br>Request: $rest<br>")
    unless (length($response));
  my $rvar;
  eval {$rvar = decode_json($response)};
  printf "Found %d images for plate %d",scalar(@{$rvar->{image_data}}),$plate;
  my %line = ();
  foreach (@{$rvar->{image_data}}) {
    (my $short = $_->{line}) =~ s/\_[A-Z]{2}_\d{2}$//;
    $vt_mapping{$short} = $_->{vt_line} if ($_->{vt_line});
    push @{$line{$short}{$_->{driver}}{$_->{area}}},[$_->{line},$_->{id},$_->{name},$_->{image_url},$_->{vt_library_match},$_->{vt_id_found},$_->{qi},$_->{annotator}];
  }
  &renderLines(\%line);
  print end_form,&sessionFooter($Session),end_html;
}


sub renderLines
{
  my($line) = shift;
  foreach my $short (sort keys %$line) {
    my $line_block .= h1($short
        . ((exists $vt_mapping{$short}) ? " ($vt_mapping{$short})" : ''));
    my %dblock = ();
    my %mismatch = ();
    foreach my $driver (sort keys %{$line->{$short}}) {
      next if (($driver eq 'LexA') && ($DRIVER ne 'All'));
      my %ablock = ();
      foreach my $area (sort keys %{$line->{$short}{$driver}}) {
        ($ablock{$area},$mismatch{$area}) = &renderArea($line->{$short}{$driver}{$area});
      }
      unless (scalar(keys %ablock) == 2) {
        $ablock{(exists $ablock{Brain}) ? 'VNC' : 'Brain'} = div({class => 'alert alert-danger'},'No<br>imagery');
      }
      foreach (sort keys %ablock) {
        my($style) = ($mismatch{$_}) ? ('background-color: ' . $mismatch{$_}) : '';
        $dblock{$driver} .= div({class => 'area',style => $style},h3($_),$ablock{$_});
      }
    }
    if ((scalar(keys %dblock) != 2) && ($DRIVER eq 'All')) {
      $dblock{(exists $dblock{LexA}) ? 'GAL4_Collection' : 'LexA'} = div({class => 'alert alert-danger'},'No areas imaged');
    }
    foreach (sort keys %dblock) {
      $line_block .= div({class => 'driver'},div({style => 'float: left'},h2($_),$dblock{$_}) . $CLEAR);
    }
    print div({class => 'line'},div({style => 'float: left; width: 100%'},$line_block) . $CLEAR);
  }
}


sub renderArea
{
  my($image_list) = shift;
  my @columns = ();
  my($mismatches,$novts) = (0)x2;
  foreach (@$image_list) {
    my($line,$image_id,$name,$url,$vtmatch,$vtid,$qi,$annotator) = @$_;
    # Get secondary images
    my $rest = $CONFIG{sage}{url}.'secondary_images?'
               . 'product=projection_all&image_id=' . $image_id . '&'
               . '_columns=url';
    my $response = get $rest;
    &terminateProgram("<h3>REST GET returned null response</h3>"
                      . "<br>Request: $rest<br>")
      unless (length($response));
    my $rvar;
    eval {$rvar = decode_json($response)};
    my $src = $rvar->{secondary_image_data}[0]{url};
    my %checked = ();
    $checked{checked}++ if ($annotator);
    # Get line ID
    $rest = $CONFIG{sage}{url}.'lines?'
            . 'name=' . $line . '&'
            . '_columns=id';
    $response = get $rest;
    &terminateProgram("<h3>REST GET returned null response</h3>"
                      . "<br>Request: $rest<br>")
      unless (length($response));
    eval {$rvar = decode_json($response)};
    my $line_id = $rvar->{line_data}[0]{id};
    my $checkbox = input({&identify($image_id),
                          type => 'checkbox',
                          class => 'selector',
                          value => $image_id,
                          onclick => 'designate("'.$image_id.'","'.$line_id
                                     .'","'.$name.'","'.$USERID.'");',
                          %checked});
    $vtmatch ||= '';
    $vtid ||= '';
    my $vt;
    if ($vtmatch eq 'Y') {
      $vt = 'VT match';
    }
    elsif ($vtid eq 'Y') {
      $vt = 'VT mismatch';
    }
    else {
      $vt = 'No VT';
    }
    $qi = (sprintf 'Q<sub>i</sub> %.2f',$qi) if ($qi);
    push @columns,table({class => 'thumb_image'},
                        Tr(td({colspan=>3},
                        a({href => "view_sage_imagery.cgi?"
                                   . "_op=stack;_family=dickson;_image=$name",
                           target => '_blank'},
                          img({src => $src,height => 180})))),
                        Tr(td({width=>'50%'},$vt),td({width=>'20%'},$checkbox),
                           td({width=>'30%'},$qi)));
    $mismatches++ if ($vt eq 'VT mismatch');
    $novts++ if ($vt eq 'No VT');
  }
  my $html = table(Tr(td([@columns])));
  my $color = '';
  $color = '#663366' if ($mismatches == scalar(@$image_list));
  $color = '#666600' if (($novts + $mismatches) == scalar(@$image_list));
  return($html,$color);
}


sub printHeader {
  my($onload) = shift;
  my @scripts = map { {-language=>'JavaScript',-src=>"/js/$_.js"} }
                    ('highcharts-4.0.1/highcharts',
                     'highcharts-4.0.1/highcharts-more',
                     $PROGRAM);
  my %load = ();
  $load{load} = $onload if ($onload);
  print &standardHeader(title => $APPLICATION,
                        script => \@scripts,
                        css_prefix => $PROGRAM,
                        breadcrumbs => \@BREADCRUMBS,
                        expires => 'now',
                        %load),
        start_multipart_form;
}
