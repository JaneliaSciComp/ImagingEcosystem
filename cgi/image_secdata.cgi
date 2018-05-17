#!/usr/bin/perl

use strict;
use warnings;
use CGI qw/:standard :cgi-lib/;
use CGI::Carp qw(fatalsToBrowser);
use CGI::Session;
use JSON;
use LWP::Simple;
use Switch;
use JFRC::Utils::Web qw(:all);

# ****************************************************************************
# * Environment-dependent                                                    *
# ****************************************************************************
# Change this on foreign installation
use constant DATA_PATH => '/opt/informatics/data/';

# ****************************************************************************
# * Constants                                                                *
# ****************************************************************************
(my $PROGRAM = (split('/',$0))[-1]) =~ s/\..*$//;
our $APPLICATION = 'Imagery secondary data';
my @BREADCRUMBS = ('Imagery tools',
                   'http://informatics-prod.int.janelia.org/#imagery');
my (%CONFIG,%SERVER);
use constant NBSP => '&nbsp;';
my $CLEAR = div({style=>'clear:both;'},NBSP);

# ****************************************************************************
# * Globals                                                                  *
# ****************************************************************************
# Web
our ($USERID,$USERNAME);
my $Session;
my $AUTHORIZED = 0;

# ****************************************************************************
# Session authentication
# ****************************************************************************
$Session = &establishSession(css_prefix => $PROGRAM);
&sessionLogout($Session) if (param('logout'));
$USERID = $Session->param('user_id');
$USERNAME = $Session->param('user_name');
$AUTHORIZED = 1 if (($Session->param('scicomp'))
                    || ($Session->param('workstation_flylight')));
&terminateProgram('You are not authorized to view Workstation imagery')
  unless ($AUTHORIZED);


# ****************************************************************************
# * Main                                                                     *
# ****************************************************************************
&initializeProgram();
if (param('id') || param('lsm')) {
  &displaySecdata();
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
  # Servers
  $file = DATA_PATH . 'servers.json';
  open SLURP,$file or &terminateProgram("Can't open $file: $!");
  sysread SLURP,my $slurp,-s SLURP;
  close(SLURP);
  my $hr = decode_json $slurp;
  %SERVER = %$hr;
}


sub displayInput
{
  &printHeader();
  print 'Enter an LSM name ',
        input({&identify('lsm'),size => 40}),
        br,(NBSP)x30,' - or -',br,
        'a Sample ID ',
        input({&identify('id'),size => 24}),br
        div({align => 'center'},
            submit({&identify($_.'_search'),
                   class => 'btn btn-success',
                   value => 'Search'}));
  print end_form,&sessionFooter($Session),end_html;
}


sub displaySecdata
{
  &printHeader();
  my $query;
  if ($query =  param('lsm')) {
    $query =~ s/.+\///;
    $query =~ s/\.bz2$//;
    my $rest = $CONFIG{jacs}{url}.$CONFIG{jacs}{query}{LSMImages} . '?name=' . $query;
    my $response = get $rest;
    &terminateProgram("<h3>REST GET returned null response</h3>"
                      . "<br>Request: $rest<br>")
      unless (length($response));
    my $rvar;
    eval {$rvar = decode_json($response)};
    $query = '';
    if (exists $rvar->{sample}) {
      ($query = $rvar->{sample}) =~ s/.+#//;
    }
  }
  elsif ($query =  param('id')) {
  }
  if ($query) {
    my $rest = $CONFIG{jacs}{url}.$CONFIG{jacs}{query}{SampleJSON} . '?sampleId=' . $query;
    my $response = get $rest;
    &terminateProgram("<h3>REST GET returned null response</h3>"
                      . "<br>Request: $rest<br>")
      unless (length($response));
    my $rvar;
    eval {$rvar = decode_json($response)};
    foreach (@{$rvar}) {
      print h1('Sample ' . $_->{name}) . br;
      foreach my $os (@{$_->{objectiveSamples}}) {
        print h2(span({class => "label label-info"},$os->{objective} . ' objective')),br,
        div({style => 'margin-left: 20px'},
            &processObjectiveSample($os));
      }
    }
  }
  print end_form,&sessionFooter($Session),end_html;
}


sub processObjectiveSample
{
  my($os) = shift;
  my $obj = '';
  # Process by tile
  my @lsms;
  foreach my $tile (@{$os->{tiles}}) {
    my $set = '';
    foreach my $lsm (@{$tile->{lsmReferences}}) {
      $set .= &renderSingleLSM($lsm->{name});
    }
    if (exists($tile->{files}) && scalar(keys %{$tile->{files}})) {
      $set .= &renderFileBlock({files => $tile->{files}});
    }
    push @lsms,$set;
  }
  $obj .= div({class => 'boxed'},h1('LSMs'),join(hr,@lsms)).br if (scalar @lsms);
  # Process by pipeline runs
  foreach my $pr (@{$os->{pipelineRuns}}) {
    $obj .= div({class => 'boxed'},h1($pr->{name}),
                h3($pr->{pipelineProcess},'run on',$pr->{creationDate}),br,
                div({style => 'margin-left: 20px;'},
                    &renderPipelineRunResults($pr))) . $CLEAR;
  }
  return($obj);
}


sub renderPipelineRunResults
{
  my($pr) = shift;
  my $r = '';
  foreach my $res (@{$pr->{results}}) {
    if (exists $res->{groups}) {
      my $g = '';
      foreach (@{$res->{groups}}) {
        $g .= div({style => 'margin-left: 20px'},
                  h3($_->{key}),
                  &renderFileBlock($_));
      }
      $r .= div(h2($res->{name}),$g);
    }
    elsif (exists $res->{files}) {
      $r .= div(h2($res->{name}),
                div({style => 'margin-left: 20px'},
                    &renderFileBlock($res)));
    }
  }
  return($r);
}


sub renderFileBlock
{
  my $res = shift;
  my $base = $res->{filepath} . '/';
  my $files = $res->{files};
  my $block = '';
  my (%name,%url);
  foreach (sort keys %{$files}) {
    my $thumb;
    my $url = $SERVER{'jacs-storage'}{address} . $base . $files->{$_};
    switch ($_) {
      case 'LSM Metadata' { $thumb = '/images/notes.jpg' }
      case /(Label|Lossless)/ { $thumb = '/images/stack_multi.png' }
      case /Movie/ { $thumb = '/images/movie_mp4.png' }
      case /Fast-loading Stack/ { $thumb = '/images/movie_mp4.png';
                                   $url = $SERVER{'jacs-storage'}{address} . $files->{$_}; }
      else { $thumb = $SERVER{'jacs-storage'}{address} . $base . $files->{$_};
             if ($_ eq 'Reference MIP') {
               $name{image1} = $_;
               $url{image1} = $url;
             }
             elsif ($_ eq 'Signal MIP') {
               $name{image2} = $_;
               $url{image2} = $url;
             }
             $url = "view_image.cgi?url=$url&caption=$_";
           }
    }
    my $img .= table({},
                     Tr(td(a({href => $url,
                              target => '_blank'},
                             img({src => $thumb,
                                  height => 100})))),
                     Tr(td($_)));
    $block .= div({class => 'single_mip'},$img);
  }
  if (exists($url{image1}) && exists($url{image2})) {
    my $p = join('&',map { $_.'name='.$name{$_}.'&'.$_.'url='.$url{$_}.'&'} 
                         keys %name);
    my $images = join(' and ',sort values %name);
    $p .= "&caption=Overlay of $images";
    my $title = "Display overlay of $images";
    $block .= div({class => 'single_mip'},
                  table({},
                        Tr(td({map {$_ => 100} qw(height width)},
                              a({href => "image_overlay.cgi?$p",
                                 target => '_blank'},$title))),
                        Tr(td('Overlay'))));
  }
  div({style => 'float: left;'},
      div({class => 'line',
           style => 'float: left;'},$block))
   . $CLEAR;
}


sub renderSingleLSM
{
  my($lsm_name) = shift;
  my $rest = $CONFIG{jacs}{url}.$CONFIG{jacs}{query}{LSMImages} . '?name=' . $lsm_name;
  my $response = get $rest;
  &terminateProgram("<h3>REST GET returned null response</h3>"
                    . "<br>Request: $rest<br>")
    unless (length($response));
  my $rvar;
  eval {$rvar = decode_json($response)};
  # Render
  my $button = '';
  if ($rvar->{files}) {
    foreach (sort keys %{$rvar->{files}}) {
      next unless ($rvar->{files}{$_} =~ /\.lsm(?:\.bz2)?$/);
      my $url = '';
      my @f = split ('/',$rvar->{files}{$_});
      my $name = join('/',$f[-2],$f[-1]);
      $rest = "$CONFIG{sage}{url}images?name=$name";
      $rest =~ s/\.bz2$//;
      $response = get $rest;
      my $rvar2;
      eval {$rvar2 = decode_json($response)};
      $url = $rvar2->{image_data}[0]{image_url} if ($rvar2);
      $button = button(-value => 'Download LSM',
                       -class => 'smallbutton',
                       -style => 'background: #393',
                       -onclick => 'window.open("' . $url . '");')
        if ($url);
      last;
    }
  }
  div({},
      h3($lsm_name
         . ' (' . join(' ',$rvar->{objective},$rvar->{anatomicalArea})
         .  ') ' . $button));
}


sub printHeader {
  print &standardHeader(title => $APPLICATION,
                        css_prefix => $PROGRAM,
                        breadcrumbs => \@BREADCRUMBS,
                        expires => 'now'),
        start_multipart_form;
}
