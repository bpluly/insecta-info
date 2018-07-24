#!python2
import sys, os
abspath = os.path.dirname(__file__)
sys.path.append(abspath)
os.chdir(abspath)


import web
from web import form
from web import utils

render = web.template.render('templates/')
validEmail = form.regexp('[^@]+@[^@]+\.[^@]+','This must be a valid email address')

urls = (
    '/', 'index',
    '/', 'index'
)

contactForm = form.Form(
    form.Textbox("Name",form.notnull),
    form.Textbox("Email",form.notnull),
    form.Textarea("Message", form.notnull))
    

class index:
      def GET(self):
        contact = contactForm()
        return render.index(contact)
        
      def POST(self):
        contact = contactForm()
        if not contact.validates():
          return render.index(contact)
        else:
          name = contact.Name.get_value()
          email = contact.Email.get_value()
          message = contact.Message.get_value()
          web.sendmail("web@objective2k.com","simon.lucy@g30consultants.com","G30 Web Message",name+" "+email+" sent "+message)
# Clear the form fields
          contact.Name.set_value("")
          contact.Email.set_value("")
          contact.Message.set_value("")
          return render.index(contact)

# wsgi magic, mod_wsgi expects the application to have an entry point called application
app = web.application(urls, globals(), autoreload=False)
application = app.wsgifunc()

