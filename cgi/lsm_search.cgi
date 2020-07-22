#!/usr/bin/perl

use strict;
use warnings;
use CGI qw/:standard :cgi-lib/;
use CGI::Carp qw(fatalsToBrowser);
use CGI::Session;
use IO::File;
use JSON;
use LWP::Simple;
use JFRC::Utils::Web qw(:all);

# ****************************************************************************
# * Environment-dependent                                                    *
# ****************************************************************************
# Change this on foreign installation
use constant DATA_PATH => '/groups/scicompsoft/informatics/data/';

# ****************************************************************************
# * Constants                                                                *
# ****************************************************************************
(my $PROGRAM = (split('/',$0))[-1]) =~ s/\..*$//;
our $APPLICATION = 'LSM search';
my @BREADCRUMBS = ('Imagery tools',
                   'http://informatics-prod.int.janelia.org/#imagery');
use constant ANY => '(any)';
use constant NBSP => '&nbsp;';

# ****************************************************************************
# * Globals                                                                  *
# ****************************************************************************
# Web
our ($USERID,$USERNAME);
my $Session;
# Configuration
my %REST;

# ****************************************************************************
# Session authentication
# ****************************************************************************
$Session = &establishSession(css_prefix => $PROGRAM);
&sessionLogout($Session) if (param('logout'));
$USERID = $Session->param('user_id');
$USERNAME = $Session->param('user_name');


# ****************************************************************************
# * Main                                                                     *
# ****************************************************************************
&initializeProgram();
if (param('date')) {
  &displayResults();
}
else {
  &showQuery();
}
# We're done!
exit(0);


# ****************************************************************************
# * Subroutines                                                              *
# ****************************************************************************

sub initializeProgram
{
  # Get general REST config
  my $file = DATA_PATH . 'rest_services.json';
  open SLURP,$file or &terminateProgram("Can't open $file: $!");
  sysread SLURP,my $slurp,-s SLURP;
  close(SLURP);
  my $hr = decode_json $slurp;
  %REST = %$hr;
}


sub showQuery
{
  &printHeader('initial');
  my $url = $REST{jacs}{url} . "info/dataset/sageimagery";
  my $rvar = &getREST($url);
  my @ds_list;
  ($_->{identifier}) && push @ds_list,$_->{identifier} foreach (@$rvar);
  unshift @ds_list,ANY;
  print table(Tr(td(['Data set: ',
                     popup_menu(&identify('dataset'),
                                -values => \@ds_list)])),
              Tr(td(['Slide code date: ',
                     input({&identify('date'),type => 'text',size => 10})])),
              Tr(td(['Slide number: ',
                     input({&identify('slidenum'),type => 'text',size => 5}). '(optional)'])),
             ),
        br,
        div({align => 'center'},
            submit({class => 'submitbutton'})),
        end_form,
        &sessionFooter($Session),end_html;
}


sub displayResults
{
  &printHeader();
  my $date = param('date');
  my $slidenum = param('slidenum');
  my $dataset = param('dataset') || ANY;
  my $subtitle = ($dataset eq ANY) ? '' : br . $dataset;
  my $code = $date;
  $code .= "_$slidenum" if ($slidenum);
  print div({align => 'center'},h2("LSMs with slide codes dated $code"
                                   . $subtitle)),br;
  my $url = $REST{sage}{url} . "/images?slide_code=$code*";
  $url .= "&data_set=$dataset" unless ($dataset eq ANY);
  $url .= '&_columns=slide_code,family,data_set,name,renamed_by,published_to,publishing_name&_sort=slide_code,data_set,name';
  my $rvar = &getREST($url);
  my $ar;
  if ($rvar && $rvar->{rest}{row_count}) {
    foreach (@{$rvar->{image_data}}) {
      $url = "view_sage_imagery.cgi?_op=stack;_family="
             . $_->{family} . "&_image=$_->{name}";
      my $sage = button(-value => 'SAGE',
                        -class => 'smallbutton',
                        -style => 'background: #0ff',
                        -onclick => 'window.open("' . $url . '");');
      (my $lsm = $_->{name}) =~ s/.+\///;
      $url = "image_secdata.cgi?lsm=$lsm";
      my $ws = button(-value => 'Workstation',
                      -class => 'smallbutton',
                      -style => 'background: #f60',
                      -onclick => 'window.open("' . $url . '");');
      my $ew = '';
      if ($_->{published_to} && $_->{publishing_name}) {
        if ($_->{published_to} == 'Split GAL4') {
          $url = "http://splitgal4.janelia.org/cgi-bin/view_splitgal4_imagery.cgi?line=";
        }
        else {
          $url = "http://flweb.janelia.org/cgi-bin/view_flew_imagery.cgi?line=";
        }
        $url .= $_->{publishing_name};
        $ew = button(-value => 'External',
                     -class => 'smallbutton',
                     -style => 'background: #6c6',
                     -onclick => 'window.open("' . $url . '");');
      }
      push @$ar,[$_->{slide_code},$_->{data_set},$_->{name},$_->{renamed_by},"$sage $ws $ew"];
    }
    my $t = table({id => 'lsms',class => 'tablesorter standard'},
                  thead(Tr(th(['Slide code','Data set','Image name','Imaged by','View on']))),
                  tbody(map {Tr(td($_))} @$ar));
    print $t,div({style => 'clear: both;'},NBSP);
  }
  else {
    print &bootstrapAlert('No LSMs found','danger');
  }
  print end_form,&sessionFooter($Session),end_html;
}


sub getREST
{
  my($rest) = shift;
  my $ua = LWP::UserAgent->new;
  my $response = $ua->get($rest);
  my $rvar;
  eval {$rvar = decode_json($response->content())};
  &terminateProgram("<h3>REST GET decode failed</h3><br>Request: $rest<br>"
                    . "Response: " . $response->status_line() . "<br>Error: $@") if ($@);
  if ($response->is_success()) {
    return($rvar);
  }
  else {
    return('') if ($response->code() == 404);
    if ($response->status_line()) {
      my $err = "<h3>REST GET failed</h3><br>Request: $rest<br>"
                . "Response: " . $response->code() . "<br>Error: " . $response->message();
      $err .= '<br>Details: ' . $rvar->{rest}{error} if (exists $rvar->{rest});
      &terminateProgram($err);
    }
  }
}


sub printHeader {
  my($type) = shift || '';
  my @js = ($type eq 'initial')
    ? ($PROGRAM) : ('jquery/jquery.tablesorter','tablesorter');
  my @scripts = map { {-language=>'JavaScript',-src=>"/js/$_.js"} } @js;
  push @scripts,{-language=>'JavaScript',-src=>"https://code.jquery.com/ui/1.12.1/jquery-ui.min.js"}
    if ($type eq 'initial');
  my @styles = (Link({-rel=>'stylesheet',
                      -type=>'text/css',-href=>"https://code.jquery.com/ui/1.12.1/themes/cupertino/jquery-ui.css"}));
  push @styles,map { Link({-rel=>'stylesheet',
                           -type=>'text/css',-href=>"/css/$_.css"}) }
                   qw(tablesorter-jrc1);
  my %load = ($type eq 'initial') ? () : (load => 'tableInitialize();');
  print &standardHeader(title => $APPLICATION,
                        css_prefix => $PROGRAM,
                        script => \@scripts,
                        style => \@styles,
                        breadcrumbs => \@BREADCRUMBS,
                        expires => 'now',
                        %load),
        start_multipart_form;
}
