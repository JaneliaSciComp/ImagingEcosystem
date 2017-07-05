function designate(image_id,line_id,image_name,user_name) {
  var status = $('#'+image_id).is(':checked');
  if (status) {
    fetch('http://informatics-flask-dev.int.janelia.org:83/sage_responder/session',{
          method: 'post',
          mode: 'cors',
          headers: { 'Access-Control-Allow-Origin': '*',
                     'Content-Type': 'application/json' },
          body: JSON.stringify({cv: 'flylight_public_annotation',
                                type: 'annotation_image',
                                name: image_name,
                                line_id: line_id,
                                image_id: image_id,
                                annotator: user_name,
                                lab: 'flylight'})
          }).then(function(response) {
            return response.json();
          }).then(function(data) {
            console.log(data.row_count);
         });
  }
  else {
    fetch('http://informatics-flask-dev.int.janelia.org:83/sage_responder/sessions?'
          + 'cv=flylight_public_annotation&type=annotation_image&lab=flylight&'
          + 'line_id=' + line_id + '&image_id=' + image_id,{
          method: 'get',
          mode: 'cors',
          headers: { 'Access-Control-Allow-Origin': '*' }
          }).then(function(response) {
            return response.json();
          }).then(function(data) {
            session_id = data.session_data[0].id;
            fetch('http://informatics-flask-dev.int.janelia.org:83/sage_responder/session/'
                  + session_id,{
                  method: 'delete',
                  mode: 'cors',
                  headers: { 'Access-Control-Allow-Origin': '*' }
                  }).then(function(response) {
                    return response.json();
                  }).then(function(data) {
                    alert('Deselected '+image_name);
                 });
         });
  }
}
