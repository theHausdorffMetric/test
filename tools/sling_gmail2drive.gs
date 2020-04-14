/* Gmail to drive
This google script allow to save mail to google drive as pdf.
These emails are received by Shan Dupont from EMCSG.
The script is located in LNG Data > LNG Pricing > sling_gmail2drive :
https://docs.google.com/spreadsheets/d/1DBWWDvnbE5QNurnsF5ecpcBOwf9IouzbDffOs_R9B0Y/
See SLING spider for more informations.
*/
function saveGmailAsPDF() {

  var folderId  = "1x4o3ns4pS9H7tEh2DXjsVWmv_pwkAfKf";
  var searchFrom = "helpdesk@emcsg.com";
  var searchSubject = "Sling Index Value Notification";
  var archiveLabel = 'sling_processed'
  var moveToLabel =  GmailApp.getUserLabelByName(archiveLabel);

  var threads = GmailApp.search("from:" + searchFrom + ' -label:'
                                + archiveLabel + 'subject:' + searchSubject);

  if (threads.length > 0) {

    /* Google Drive folder where the Files would be saved */
    var folder = DriveApp.getFolderById(folderId);

    for (var t=0; t<threads.length; t++) {

      var msgs = threads[t].getMessages();

      for (var m=0; m<msgs.length; m++) {
        var msg = msgs[m];

        var date = msg.getDate();
        var formattedDate = Utilities.formatDate(date, "GMT", "yyyy-MM-dd'T'HH:mm:ss");
        var html = msg.getBody().replace(/<img[^>]*>/g,"");

        /* Conver the Email Message into a PDF File */
        var tempFile = DriveApp.createFile("temp.html", html, "text/html");
        folder.createFile(tempFile.getAs("application/pdf")).setName(formattedDate);
        tempFile.setTrashed(true);
      }

      /* Add label to not process the email during the next run*/
      threads[t].addLabel(moveToLabel);

    }
  }
}
