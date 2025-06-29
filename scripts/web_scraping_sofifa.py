import pandas as pd
import numpy as np
import time
import random
import re
import psutil
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
from bs4 import BeautifulSoup
from kafka import KafkaProducer
import json
import logging
from requests.exceptions import ReadTimeout

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SofifaScraper:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:126.0) Gecko/20100101 Firefox/126.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        self.user_agents = [
            'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:126.0) Gecko/20100101 Firefox/126.0',
            'Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0',
            'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:124.0) Gecko/20100101 Firefox/124.0'
        ]

    def generate_urls(self, max_pages=5):
        """Génère les URLs pour le scraping (300 joueurs / 5 pages)"""
        base_url = "https://sofifa.com/players?&showCol%5B%5D=pi&showCol%5B%5D=ae&showCol%5B%5D=hi&showCol%5B%5D=wi&showCol%5B%5D=pf&showCol%5B%5D=oa&showCol%5B%5D=pt&showCol%5B%5D=bo&showCol%5B%5D=bp&showCol%5B%5D=gu&showCol%5B%5D=vl&showCol%5B%5D=wg&showCol%5B%5D=rc&showCol%5B%5D=ta&showCol%5B%5D=cr&showCol%5B%5D=fi&showCol%5B%5D=he&showCol%5B%5D=sh&showCol%5B%5D=vo&showCol%5B%5D=ts&showCol%5B%5D=dr&showCol%5B%5D=cu&showCol%5B%5D=fr&showCol%5B%5D=lo&showCol%5B%5D=bl&showCol%5B%5D=to&showCol%5B%5D=ac&showCol%5B%5D=sp&showCol%5B%5D=ag&showCol%5B%5D=re&showCol%5B%5D=ba&showCol%5B%5D=tp&showCol%5B%5D=so&showCol%5B%5D=ju&showCol%5B%5D=st&showCol%5B%5D=sr&showCol%5B%5D=ln&showCol%5B%5D=te&showCol%5B%5D=ar&showCol%5B%5D=in&showCol%5B%5D=po&showCol%5B%5D=vi&showCol%5B%5D=pe&showCol%5B%5D=cm&showCol%5B%5D=td&showCol%5B%5D=ma&showCol%5B%5D=sa&showCol%5B%5D=sl&showCol%5B%5D=tg&showCol%5B%5D=gd&showCol%5B%5D=gh&showCol%5B%5D=gc&showCol%5B%5D=gp&showCol%5B%5D=gr&showCol%5B%5D=tt&showCol%5B%5D=bs&showCol%5B%5D=ir&showCol%5B%5D=pac&showCol%5B%5D=sho&showCol%5B%5D=pas&showCol%5B%5D=dri&showCol%5B%5D=def&showCol%5B%5D=phy&offset="
        urls = []
        for page in range(0, max_pages * 60, 60):
            urls.append(f"{base_url}{page}")
        return urls
    
    def parse_value(self, value_str):
        """Parse les valeurs monétaires (€28.5M -> 28500000)"""
        if not value_str or value_str == '-':
            return 0
        value_str = value_str.replace('€', '').replace(',', '')
        if 'M' in value_str:
            return float(value_str.replace('M', '')) * 1000000
        elif 'K' in value_str:
            return float(value_str.replace('K', '')) * 1000
        else:
            try:
                return float(value_str)
            except:
                return 0
    
    def parse_numeric(self, text):
        """Extract the first numeric value from text (e.g., '80-1' -> 80)"""
        if not text:
            return 0
        match = re.match(r'(\d+)', text.strip())
        return int(match.group(1)) if match else 0
    
    def parse_team(self, team_cell, contract_cell):
        """Parse le nom de l'équipe, gérant les cas comme 'Free'"""
        team_text = team_cell.find('a').text.strip() if team_cell.find('a') else ''
        contract_text = contract_cell.text.strip() if contract_cell else ''
        if 'Free' in contract_text or team_text == '':
            return 'Free'
        return team_text
    
    def categorize_position(self, position):
        """Catégorise la position en Gardien, Attaquant, Défenseur, Milieu"""
        if position == 'GK':
            return 'gardien'
        elif position in ['CF', 'ST', 'RW', 'LW']:
            return 'attaquant'
        elif position in ['CB', 'LB', 'RB', 'RWB', 'LWB']:
            return 'défenseur'
        elif position in ['CM', 'CDM', 'CAM', 'LM', 'RM']:
            return 'milieu'
        return position.lower()
    
    def cleanup_processes(self):
        """Terminer les processus GeckoDriver et Firefox restants"""
        for proc in psutil.process_iter(['name']):
            try:
                if proc.name() in ['geckodriver', 'firefox']:
                    proc.terminate()
                    proc.wait(timeout=3)
                    logger.info(f"Terminated process: {proc.name()}")
            except (psutil.NoSuchProcess, psutil.TimeoutExpired):
                pass
    
    def scrape_page(self, url, retries=2):
        """Scrape une page de joueurs avec re-tentatives"""
        for attempt in range(retries):
            driver = None
            try:
                user_agent = random.choice(self.user_agents)
                firefox_options = Options()
                firefox_options.add_argument("--headless")
                firefox_options.add_argument(f"user-agent={user_agent}")
                service = Service('/home/sami/opt/geckodriver')
                driver = webdriver.Firefox(service=service, options=firefox_options)
                driver.set_page_load_timeout(300)
                
                driver.get(url)
                time.sleep(random.uniform(5, 10))
                
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                
                players = []
                table = soup.find('table')
                
                if not table:
                    logger.warning(f"Aucun tableau trouvé sur {url}")
                    return players
                
                rows = table.find('tbody').find_all('tr')
                
                for row in rows:
                    try:
                        cells = row.find_all('td')
                        if len(cells) < 66:
                            continue
                        
                        player_data = {}
                        
                        # Nom du joueur
                        name_cell = cells[1].find('a')
                        player_data['Name'] = name_cell.text.strip() if name_cell else ''
                        
                        # ID du joueur
                        player_data['ID'] = cells[6].text.strip() if cells[6] else ''
                        
                        # Âge
                        player_data['Age'] = int(cells[2].text.strip()) if cells[2].text.strip().isdigit() else 0
                        
                        # Overall rating
                        player_data['Overall rating'] = self.parse_numeric(cells[3].text.strip())
                        
                        # Potential
                        player_data['Potential'] = self.parse_numeric(cells[4].text.strip())
                        
                        # Équipe
                        player_data['Team'] = self.parse_team(cells[5], cells[12])
                        
                        # Position category
                        best_position = cells[11].text.strip() if cells[11] else ''
                        player_data['Position category'] = self.categorize_position(best_position)
                        
                        # Valeur
                        player_data['Value'] = self.parse_value(cells[13].text.strip())
                        
                        # Salaire
                        player_data['Wage'] = self.parse_value(cells[14].text.strip())
                        
                        # Total fields
                        total_fields = {
                            'Total attacking': 16,
                            'Total skill': 22,
                            'Total movement': 28,
                            'Total power': 34,
                            'Total mentality': 40,
                            'Total defending': 47,
                            'Total goalkeeping': 51
                        }
                        for field, index in total_fields.items():
                            player_data[field] = int(cells[index].text.strip()) if cells[index].text.strip().isdigit() else 0
                        
                        # Stats principales
                        player_data['Pace / Diving'] = int(cells[60].text.strip()) if cells[60].text.strip().isdigit() else 0
                        player_data['Shooting / Handling'] = int(cells[61].text.strip()) if cells[61].text.strip().isdigit() else 0
                        player_data['Passing / Kicking'] = int(cells[62].text.strip()) if cells[62].text.strip().isdigit() else 0
                        player_data['Dribbling / Reflexes'] = int(cells[63].text.strip()) if cells[63].text.strip().isdigit() else 0
                        player_data['Defending / Pace'] = int(cells[64].text.strip()) if cells[64].text.strip().isdigit() else 0
                        
                        # Filtrer Value et Wage != 0
                        if player_data['Value'] == 0 or player_data['Wage'] == 0:
                            continue
                        
                        players.append(player_data)
                        
                    except Exception as e:
                        logger.error(f"Erreur lors du parsing d'un joueur: {e}")
                        continue
                
                logger.info(f"Scraped {len(players)} joueurs depuis {url}")
                return players
                
            except (ReadTimeout, TimeoutError) as e:
                logger.warning(f"Timeout pour {url} (tentative {attempt+1}/{retries}): {e}")
                if attempt < retries - 1:
                    time.sleep(random.uniform(10, 15))
                    continue
                logger.error(f"Échec après {retries} tentatives pour {url}: {e}")
                return []
            except Exception as e:
                logger.error(f"Erreur générale pour {url}: {e}")
                return []
            finally:
                if driver:
                    try:
                        driver.quit()
                        time.sleep(2)
                    except Exception as e:
                        logger.warning(f"Erreur lors de la fermeture du driver: {e}")
                    finally:
                        self.cleanup_processes()
    
    def scrape_all_pages(self, max_pages=5):
        """Scrape toutes les pages"""
        urls = self.generate_urls(max_pages)
        all_players = []
        
        for i, url in enumerate(urls):
            logger.info(f"Scraping page {i+1}/{len(urls)}")
            players = self.scrape_page(url)
            all_players.extend(players)
            time.sleep(random.uniform(10, 15))
        
        return all_players

class KafkaPlayerProducer:
    def __init__(self, bootstrap_servers='localhost:9092', topic='players-data'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8')
        )
        self.topic = topic
    
    def send_player_data(self, players_data):
        """Envoie les données des joueurs vers Kafka"""
        for player in players_data:
            try:
                self.producer.send(
                    self.topic,
                    key=player.get('ID', ''),
                    value=player
                )
                logger.info(f"Données envoyées pour {player.get('Name', 'Unknown')}")
            except Exception as e:
                logger.error(f"Erreur envoi Kafka: {e}")
        
        self.producer.flush()
        logger.info(f"Envoi de {len(players_data)} joueurs vers Kafka terminé")

def scale_total_fields(df):
    """Met à l'échelle les champs Total entre 1 et 99"""
    total_columns = [
        'Total attacking', 'Total skill', 'Total movement', 'Total power',
        'Total mentality', 'Total defending', 'Total goalkeeping'
    ]
    for col in total_columns:
        if col in df.columns:
            min_val = df[col].min()
            max_val = df[col].max()
            if max_val > min_val:
                df[col] = (1 + ((df[col] - min_val) / (max_val - min_val) * 98)).round().astype(int)
            else:
                df[col] = 1
    return df

def merge_with_existing_data(new_data, existing_csv='True_players_data.csv', output_csv='True_players_data.csv'):
    """Fusionne les nouvelles données avec les anciennes, supprime les doublons"""
    try:
        existing_df = pd.read_csv(existing_csv)
    except FileNotFoundError:
        existing_df = pd.DataFrame(columns=new_data.columns)
    
    combined_df = pd.concat([existing_df, new_data], ignore_index=True)
    initial_rows = len(combined_df)
    combined_df = combined_df.drop_duplicates(subset=['ID'], keep='last')
    new_rows = len(combined_df) - len(existing_df)
    logger.info(f"Ajouté {new_rows} nouveaux joueurs, supprimé {initial_rows - len(combined_df)} doublons")
    
    combined_df.to_csv(output_csv, index=False)
    return combined_df

def main():
    scraper = SofifaScraper()
    logger.info("Début du scraping...")
    players_data = scraper.scrape_all_pages(max_pages=5)
    
    if not players_data:
        logger.error("Aucune donnée récupérée")
        return
    
    df = pd.DataFrame(players_data)
    df = scale_total_fields(df)
    
    # Fusionner avec les données existantes
    df = merge_with_existing_data(df)
    
    csv_filename = 'players_data.csv'
    df.to_csv(csv_filename, index=False)
    logger.info(f"Données sauvegardées dans {csv_filename}")
    
    print(f"\nNombre de joueurs totaux: {len(df)}")
    print(f"Colonnes disponibles: {list(df.columns)}")
    print(f"\nPremiers joueurs:")
    print(df.head())
    
    try:
        kafka_producer = KafkaPlayerProducer()
        kafka_producer.send_player_data(players_data)
        logger.info("Données envoyées vers Kafka avec succès")
    except Exception as e:
        logger.warning(f"Erreur Kafka (normal si Kafka n'est pas démarré): {e}")

if __name__ == "__main__":
    main()
