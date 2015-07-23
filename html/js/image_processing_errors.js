function toggleClass (cls) {
  if ($('#show_'+cls).is(':checked')) {
    $('.'+cls).show()
  }
  else {
    $('.'+cls).hide()
  }
}
