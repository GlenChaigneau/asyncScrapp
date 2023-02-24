import asyncio  # pour les requêtes asynchrones
import aiohttp  # pour les requêtes asynchrones
from bs4 import BeautifulSoup   # pour la création du fichier csv
from Notary import Notary   # pour la classe Notary
import pandas as pd # pour la création du fichier csv
import time # pour le timer
from tqdm import tqdm # pour la barre de progression
import re   # pour la fonction format_phone_number

baseUrl = 'https://www.notaires.fr'
uri = '/fr/directory/notaries?location=lyon&lat=45.758&lon=4.835&page='
pageNb = 10


async def swoup(session, url):
    async with session.get(url) as response:
        return BeautifulSoup(await response.text(), 'html.parser')


async def get_endpoints(session, cards):
    tasks = []
    for card in cards:
        link = card.find("a", class_="arrow-link")['href'].split('?')[0]
        task = asyncio.create_task(swoup(session, baseUrl + link))
        tasks.append(task)
    endpoints = []
    for task in asyncio.as_completed(tasks):
        soup = await task
        endpoints.append(soup)
    return endpoints


def get_name(soup):
    try:
        return soup.find("h1", class_="office-sheet__title text-center text-m-start").find("span").text.split(" : ")[0]
    except AttributeError:
        return ""


def format_phone_number(phone_number):
    """Formate un numéro de téléphone au format 00 00 00 00 00"""
    digits = re.sub("[^0-9]", "", phone_number)  # on ne garde que les chiffres
    return " ".join(digits[i:i+2] for i in range(0, 10, 2))

def get_phone(soup):
    try:
        phone = soup.find("div", class_="office-sheet__phone field--telephone").find("a").text
        return format_phone_number(phone)
    except AttributeError:
        return ""


def get_mail(soup):
    try:
        return soup.find("div", class_="office-sheet__email field--email").find("a")['href'].replace("mailto:", "")
    except AttributeError:
        return ""


def get_website(soup):
    try:
        website = soup.find("div", class_="office-sheet__url field--link").find("a")['href']
        if website.startswith("http"):
            return website
        else:
            return "https://" + website
    except AttributeError:
        return ""


def get_address(soup):
    try:
        spans = soup.find("div", class_="office-sheet__address field--address").find("p", "address").findAll("span")
        address = " ".join([span.text for span in spans])
        return address
    except AttributeError:
        return ""


def format_notary(notary_array):
    notary_data = []
    for notary in notary_array:
        if notary:
            notary_dict = {
                'name': notary.get_name(),
                'mail': notary.get_mail(),
                'phone': notary.get_phone(),
                'website': notary.get_website(),
                'address': notary.get_address()
            }
            notary_data.append(notary_dict)

    return notary_data


def sort_notaries(notaries):
    # Créer un dictionnaire dont les clés sont les combinaisons uniques
    # d'adresse, téléphone et email et les valeurs sont des listes de notaires correspondants
    notaries_dict = {}
    for notary in notaries:
        key = (notary.address, format_phone_number(notary.phone), notary.mail)
        if key in notaries_dict:
            notaries_dict[key].append(notary)
        else:
            notaries_dict[key] = [notary]

    # Créer une liste triée de notaires en conservant uniquement la personne dont le prénom
    # et le mail concordent pour les notaires ayant la même adresse, téléphone et email
    sorted_notaries = []
    for key, notary_list in notaries_dict.items():
        if len(notary_list) == 1:
            sorted_notaries.extend(notary_list)
        else:
            # Chercher la personne dont le prénom et le mail concordent
            matching_notary = None
            for notary in notary_list:
                if notary.name:
                    if notary.name.split()[0] == notary.mail.split('@')[0]:
                        matching_notary = notary
                        break
            if matching_notary is not None:
                sorted_notaries.append(matching_notary)
            else:
                # Si aucune personne ne correspond, ajouter simplement le premier notaire de la liste
                sorted_notaries.append(notary_list[0])

    # Trier les notaires par adresse, téléphone et email
    sorted_notaries.sort(key=lambda x: (x.address, format_phone_number(x.phone), x.mail))
    return sorted_notaries


def remove_empty_rows(csv_file):
    df = pd.read_csv(csv_file)
    df.dropna(how='all', inplace=True)
    df.to_csv(csv_file, index=False)


async def main():
    async with aiohttp.ClientSession() as session:
        notaries = []
        for page in tqdm(range(0, pageNb + 1), desc="Progression"):
            final_url = baseUrl + uri + str(page)
            soup = await swoup(session, final_url)
            cards = soup.findAll("article", class_="notary-card notary-card--notary")
            endpoints = await get_endpoints(session, cards)
            for endpoint in endpoints:
                nom = get_name(endpoint)
                phone = get_phone(endpoint)
                mail = get_mail(endpoint)
                website = get_website(endpoint)
                address = get_address(endpoint)
                notary = Notary(nom, phone, mail, website, address)
                notaries.append(notary)

        sorted_notaries = sort_notaries(notaries)
        formated_notary = format_notary(sorted_notaries)
        df = pd.DataFrame(formated_notary)
        df.to_csv("notaries.csv", index=False)
        remove_empty_rows("notaries.csv")

        print(f"Nombre de notaires : {len(notaries)}")
        print(f"Nombre de notaires après le tri : {len(sorted_notaries)}")

start_time = time.time()
asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
asyncio.run(main())
end_time = time.time()

print(f"Temps d'exécution : {end_time - start_time}")

