#!/usr/bin/env python3
"""
Script simplifié pour télécharger les logos depuis les sites officiels des entreprises
ou depuis des sources publiques
"""

import requests
from pathlib import Path
import time

BASE_DIR = Path(__file__).parent.parent
LOGOS_DIR = BASE_DIR / "HTML" / "images" / "logos"
LOGOS_DIR.mkdir(parents=True, exist_ok=True)

# URLs directes vers les logos sur les sites officiels ou sources publiques
LOGO_URLS = {
    "credit_agricole": [
        "https://www.credit-agricole.fr/content/dam/ca-fr/images/logo-ca.svg",
        "https://www.credit-agricole.fr/var/ca-fr/storage/images/media/logo-ca.png",
    ],
    "societe_generale": [
        "https://www.societegenerale.com/var/societegenerale_com/storage/images/media/logos/logo-societe-generale.png",
        "https://www.societegenerale.com/content/dam/sgcom/images/logos/societe-generale-logo.svg",
    ],
    "deloitte": [
        "https://www2.deloitte.com/content/dam/Deloitte/global/Images/promo_images/gx-dtt-global-logo.svg",
        "https://www2.deloitte.com/content/dam/Deloitte/fr/Images/promo_images/logo-deloitte-france.svg",
    ],
    "caceis": [
        "https://www.caceis.com/var/caceis_com/storage/images/media/logo-caceis.svg",
    ],
    "lcl": [
        "https://www.lcl.fr/var/lcl_fr/storage/images/media/logo-lcl.svg",
    ],
    "amundi": [
        "https://www.amundi.com/var/amundi_com/storage/images/media/logo-amundi.svg",
    ],
    "bpce": [
        "https://upload.wikimedia.org/wikipedia/fr/0/0a/Groupe_BPCE_%28logo%29.svg",
    ],
}

def download_logo(url: str, filename: str) -> bool:
    """Télécharge un logo depuis une URL"""
    try:
        print(f"📥 Téléchargement de {url[:60]}...", end=" ", flush=True)
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
        }
        
        response = requests.get(url, headers=headers, timeout=15, allow_redirects=True)
        
        if response.status_code == 200 and response.content:
            # Vérifier que c'est bien une image
            content_type = response.headers.get('content-type', '')
            is_image = (
                'image' in content_type or
                'svg' in content_type or
                response.content.startswith(b'\x89PNG') or
                response.content.startswith(b'\xff\xd8\xff') or
                response.content.startswith(b'GIF') or
                b'<svg' in response.content[:200] or
                b'<?xml' in response.content[:200]
            )
            
            if is_image and len(response.content) > 100:
                # Déterminer l'extension
                ext = 'png'
                if 'svg' in content_type or b'<svg' in response.content[:200]:
                    ext = 'svg'
                elif 'jpeg' in content_type or 'jpg' in content_type:
                    ext = 'jpg'
                elif 'gif' in content_type:
                    ext = 'gif'
                
                output_path = LOGOS_DIR / f"{filename}.{ext}"
                with open(output_path, 'wb') as f:
                    f.write(response.content)
                
                file_size = output_path.stat().st_size
                if file_size > 100:
                    print(f"✅ Sauvegardé : {output_path.name} ({file_size} bytes)")
                    return True
        
        print(f"❌ Échec (HTTP {response.status_code})")
        return False
        
    except Exception as e:
        print(f"❌ Erreur : {str(e)[:50]}")
        return False

def main():
    print("🚀 Téléchargement des logos depuis les sites officiels\n")
    print(f"📁 Dossier de destination : {LOGOS_DIR}\n")
    
    success_count = 0
    failed_count = 0
    skipped_count = 0
    
    for logo_name, urls in LOGO_URLS.items():
        # Vérifier si le fichier existe déjà
        existing = list(LOGOS_DIR.glob(f"{logo_name}.*"))
        if existing:
            print(f"⏭️  {logo_name} existe déjà ({existing[0].name})")
            skipped_count += 1
            continue
        
        # Essayer chaque URL jusqu'à ce qu'une fonctionne
        downloaded = False
        for url in urls:
            if download_logo(url, logo_name):
                success_count += 1
                downloaded = True
                break
            time.sleep(0.5)  # Petit délai entre les tentatives
        
        if not downloaded:
            failed_count += 1
        
        time.sleep(1)  # Délai entre les entreprises
    
    print(f"\n{'='*60}")
    print(f"📊 Résumé :")
    print(f"   ✅ Succès : {success_count}")
    print(f"   ⏭️  Ignorés : {skipped_count}")
    print(f"   ❌ Échecs : {failed_count}")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    main()
