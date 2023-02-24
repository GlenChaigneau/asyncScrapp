class Notary:

    def __init__(self, name, phone, mail, website, address):
        self.name = name
        self.phone = phone
        self.mail = mail
        self.website = website
        self.address = address

    def get_name(self):
        return self.name

    def get_phone(self):
        return self.phone

    def get_mail(self):
        return self.mail

    def get_website(self):
        return self.website

    def get_address(self):
        return self.address