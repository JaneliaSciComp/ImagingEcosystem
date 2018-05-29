#!/usr/bin/perl

use strict;
use warnings;
use CGI qw/:standard :cgi-lib/;
use CGI::Session;

use JFRC::Utils::Web qw(:all);

# ****************************************************************************
# * Constants                                                                *
# ****************************************************************************
# General
(my $PROGRAM = (split('/',$0))[-1]) =~ s/\..*$//;
our $APPLICATION = 'Image overlay';
use constant NBSP => '&nbsp;';

# ****************************************************************************
# * Globals                                                                  *
# ****************************************************************************

# ****************************************************************************
# * Main                                                                     *
# ****************************************************************************

my $content = (param('image1url')) ? &getContent() : '';
my @scripts = map { {-language=>'JavaScript',-src=>"/js/jquery/$_.js"} }
                    (qw(jquery-latest jquery-ui-latest));
push @scripts,{-language=>'JavaScript',-src=>'/js/'.$PROGRAM.'.js'};
print header,
      start_html(-title => $APPLICATION,
                 -style => {src => '/css/image_overlay.css'},
                 -script => \@scripts),
      body({onload => 'initialize()'}),$content,end_html;
exit(0);


sub getContent
{
  my $image1url = param('image1url');
  my $image2url = param('image2url');
  my $caption = param('caption');
  my($content,$fp,$style) = ('')x3;
  my %value;
  my @control = ();
  foreach ('image1','image2') {
    my $unit = '%';
    my $max = 100;
    push @control,Tr(td([(param($_ . 'name') || ucfirst($_)) . ' opacity:',
                         '0'
                         . input({id => $_ . 'Slider',
                                  type => 'range',
                                  min => 0,
                                  max => $max,
                                  step => 1,
                                  onchange => "changeSlider('$_');"}) . $max
                                              . $unit,
                         input({id => $_ . 'Opacity',
                                type => 'text',
                                size => 4,
                                onchange => "changeBox('$_');"}).$unit]));
  }
  $content = div({class => 'imagebox'},
                 div({class => 'top_overlay'},$caption),
                 div({class => 'overlay_images'},
                     img({&identify('image1'),
                         src => $image1url}),
                     img({&identify('image2'),
                         src => $image2url}) 
                    ),
                 div({class => 'bottom_overlay'},table({class => 'controls'},@control)));
  return($content);
}
