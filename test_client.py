import asyncio
import httpx
import io
from PIL import Image

def get_fake_image_bytes():
    """Génère une image factice rouge de 256x256 pour tromper la validation d'octets."""
    img = Image.new('RGB', (256, 256), color=(255, 0, 0))
    img_byte_arr = io.BytesIO()
    img.save(img_byte_arr, format='JPEG')
    return img_byte_arr.getvalue()

async def run_tests():
    base_url = "http://127.0.0.1:8000"
    
    headers = {"X-API-Key": "meteorite_secret_prod_key_2026"}
    async with httpx.AsyncClient(base_url=base_url, headers=headers, timeout=30.0) as client:
        print("🚀 [TEST CLIENT] Démarrage de la suite E2E...\n")
        fake_img = get_fake_image_bytes()
        
        # =======================================================
        # Scénario A : Scan Extérieur Haute Confiance
        # =======================================================
        print("▶️ SCÉNARIO A : Soumission d'un scan extérieur (Flux Nominal)")
        files_exterior_req = [
            ("files_exterior", ("img1.jpg", fake_img, "image/jpeg")),
            ("files_exterior", ("img2.jpg", fake_img, "image/jpeg")),
            ("files_exterior", ("img3.jpg", fake_img, "image/jpeg")),
        ]
        data_valid = {
            "client_uuid": "sync-device-a-12345",
            "user_id": "NabilUser123",
            "weight": 25.5,
            "magnetic": True,
            "latitude": 33.5,
            "longitude": -7.5
        }
        
        resp_a = await client.post("/api/v1/scan/exterior", files=files_exterior_req, data=data_valid)
        print(f"Status HTTP : {resp_a.status_code}")
        
        scan_id = None
        if resp_a.status_code == 200:
            payload = resp_a.json()
            scan_id = payload.get("scan_id")
            print(f"✅ SCÉNARIO A RÉUSSI | Scan ID généré : {scan_id}")
            print(f"Verdict du Business Orchestrator : {payload.get('status_code')}\n")
        else:
            print(f"❌ SCÉNARIO A ÉCHOUÉ | Erreur : {resp_a.text}\n")
            return
            
        print("▶️ SCÉNARIO A1 : Test d'idempotence (Ré-envoi suite à coupure réseau)")
        files_exterior_req2 = [
            ("files_exterior", ("img1.jpg", fake_img, "image/jpeg")),
            ("files_exterior", ("img2.jpg", fake_img, "image/jpeg")),
            ("files_exterior", ("img3.jpg", fake_img, "image/jpeg")),
        ]
        resp_a1 = await client.post("/api/v1/scan/exterior", files=files_exterior_req2, data=data_valid)
        if resp_a1.status_code == 200:
            payload_a1 = resp_a1.json()
            if payload_a1.get("is_sync_retry"):
                print("✅ SCÉNARIO A1 RÉUSSI | Idempotence activée, doublon évité.")
            else:
                print("❌ SCÉNARIO A1 ÉCHOUÉ | Le serveur a relancé l'IA pour un doublon.")
        else:
            print(f"❌ SCÉNARIO A1 ÉCHOUÉ | Erreur : {resp_a1.status_code}")

        # =======================================================
        # Scénario B : L'Enrichissement Temporel (Scan en 2 temps)
        # =======================================================
        if scan_id:
            print(f"▶️ SCÉNARIO B : Ajout de la coupe interne pour {scan_id}")
            files_interior_req = {
                "file_interior": ("interior_cut.jpg", fake_img, "image/jpeg")
            }
            
            resp_b = await client.patch(f"/api/v1/scan/{scan_id}/interior", files=files_interior_req)
            print(f"Status HTTP : {resp_b.status_code}")
            
            if resp_b.status_code == 200:
                payload_b = resp_b.json()
                print("✅ SCÉNARIO B RÉUSSI | JSONB et Inférence combinés avec succès.")
                print(f"Nouveau Verdict : {payload_b.get('status_code')}\n")
            else:
                print(f"❌ SCÉNARIO B ÉCHOUÉ | Erreur : {resp_b.text}\n")

        # =======================================================
        # Scénario C : Bouclier Sanitaire (Pydantic & Exceptions)
        # =======================================================
        print("▶️ SCÉNARIO C : Test de résilience & Validation de données")
        files_corrupted_req = [
            ("files_exterior", ("error1.jpg", fake_img, "image/jpeg")),
            ("files_exterior", ("error2.jpg", fake_img, "image/jpeg")),
            ("files_exterior", ("error3.jpg", fake_img, "image/jpeg")),
        ]
        data_corrupt = {
            "client_uuid": "sync-device-c-00000",
            "user_id": "Invalid@User!", # Caractères exclus
            "latitude": 150.0 # Hors limite (-90 à 90)
        }
        
        resp_c = await client.post("/api/v1/scan/exterior", files=files_corrupted_req, data=data_corrupt)
        print(f"Status HTTP : {resp_c.status_code}")
        
        if resp_c.status_code in [400, 422]:
            print("✅ SCÉNARIO C RÉUSSI | Requête interceptée proprement par le bouclier Pydantic.")
            print(f"Payload d'erreur : {resp_c.json()}\n")
        else:
            print(f"❌ SCÉNARIO C ÉCHOUÉ | Le serveur a potentiellement crashé ou bypassé la validation. (Code {resp_c.status_code})\n")
            
        print("🎉 Tous les tests intégration backend sont terminés !")

if __name__ == "__main__":
    asyncio.run(run_tests())
