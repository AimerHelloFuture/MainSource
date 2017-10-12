# -*- coding: utf-8 -*-

from email import encoders
from email.header import Header
from email.mime.text import MIMEText
from email.utils import parseaddr, formataddr
import smtplib


class MailHelper:
    def __init__(self):
        pass

    def _format_addr(self, s):
        name, addr = parseaddr(s)
        return formataddr((
            Header(name, 'utf-8').encode(),
            addr.encode('utf-8') if isinstance(addr, unicode) else addr))

    def send_mail_to(self, dest, message, subject=u'Crawler Normal'):
        if dest is None:
            print 'dest is none'
            return

        from_addr = 'anhye_abcft@163.com'
        password = 'Pa55word'
        to_addr = dest
        smtp_server = 'smtp.163.com'

        msg = MIMEText(message, 'plain', 'utf-8')
        msg['From'] = self._format_addr(u'apes crawler <%s>' % from_addr)
        msg['To'] = self._format_addr(u'管理员 <%s>' % ','.join(dest))
        msg['Subject'] = Header(subject, 'utf-8').encode()

        server = smtplib.SMTP(smtp_server, 25)
        server.set_debuglevel(1)
        server.login(from_addr, password)
        server.sendmail(from_addr, to_addr, msg.as_string())
        server.quit()
