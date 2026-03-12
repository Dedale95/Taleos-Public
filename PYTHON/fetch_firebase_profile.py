#!/usr/bin/env python3
"""
Récupère le profil Firestore (même structure que l'extension) pour mapper
les champs aux formulaires (Deloitte Workday, etc.).

Usage:
  export GOOGLE_APPLICATION_CREDENTIALS=chemin/vers/serviceAccountKey.json
  python fetch_firebase_profile.py --email thibault.parisien@laposte.net

  # ou avec l'UID directement
  python fetch_firebase_profile.py --uid <firebase_uid>

  # chemin explicite vers la clé
  python fetch_firebase_profile.py --key ./serviceAccountKey.json --email user@example.com

Obtenir la clé : Firebase Console → Project Settings → Service accounts →
Generate new private key (fichier JSON). Ne jamais commiter ce fichier.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import sys


def decode_base64_password(b64: str | None) -> str:
    if not b64 or not isinstance(b64, str):
        return ""
    s = b64.strip()
    pad = len(s) % 4
    if pad:
        s += "=" * (4 - pad)
    try:
        return base64.b64decode(s).decode("utf-8", errors="replace")
    except Exception:
        return b64


def normalize_profile(profile_doc: dict, creds_doc: dict | None, bank_id: str = "deloitte") -> dict:
    """
    Même logique que fetchProfile() dans extension/background.js.
    Retourne le dictionnaire utilisé par le filler Deloitte (et autres).
    """
    profile = profile_doc or {}
    creds = creds_doc or {}

    # Email / mot de passe de connexion (career_connections)
    auth_email = (creds.get("email") or "").strip()
    auth_password = decode_base64_password(creds.get("password"))

    # Téléphone
    phone = (profile.get("phone") or "").strip().replace(" ", "")
    phone_country_code = profile.get("phone_country_code") or "+33"
    phone_number = phone
    if not profile.get("phone_country_code") and phone:
        if phone.startswith("+"):
            m = re.match(r"^(\+\d{1,4})(.*)$", phone)
            if m:
                phone_country_code = m.group(1)
                phone_number = (m.group(2) or "").replace(" ", "").replace("-", "") or phone
        elif phone.startswith("0") and len(phone) >= 10:
            phone_country_code = "+33"
            phone_number = phone[1:].replace(" ", "").replace("-", "")

    contract_type = profile.get("contract_type")
    contract_list = contract_type if isinstance(contract_type, list) else ([contract_type] if contract_type else [])

    languages = []
    for l in profile.get("languages") or []:
        if isinstance(l, dict):
            languages.append({"name": l.get("language") or l.get("name") or "", "level": l.get("level") or ""})
        else:
            languages.append({"name": "", "level": ""})

    return {
        "civility": profile.get("civility") or "",
        "firstname": profile.get("first_name") or "",
        "lastname": profile.get("last_name") or "",
        "email": profile.get("email") or auth_email or "",
        "address": profile.get("address") or "",
        "zipcode": str(profile.get("postal_code") or ""),
        "city": profile.get("city") or "",
        "country": profile.get("country") or "",
        "phone_country_code": phone_country_code,
        "phone_number": phone_number or phone,
        "phone-number": profile.get("phone") or "",
        "job_families": profile.get("jobs") or [],
        "contract_types": contract_list,
        "available_date": profile.get("available_from") or profile.get("available_from_raw") or "",
        "continents": profile.get("continents") or [],
        "target_countries": profile.get("preferred_countries") or [],
        "target_regions": profile.get("regions") or [],
        "experience_level": profile.get("experience_level") or "",
        "education_level": profile.get("education_level") or "",
        "school_type": profile.get("institution_type") or "",
        "diploma_status": profile.get("diploma_status") or "",
        "diploma_year": str(profile.get("graduation_year") or ""),
        "languages": languages,
        "cv_storage_path": profile.get("cv_storage_path"),
        "lm_storage_path": profile.get("letter_storage_path"),
        "cv_filename": profile.get("cv_filename") or (profile.get("cv_storage_path") or "").split("/")[-1] or None,
        "lm_filename": profile.get("letter_filename") or (profile.get("letter_storage_path") or "").split("/")[-1] or None,
        "auth_email": auth_email,
        "auth_password": auth_password,
        "deloitte_worked": profile.get("deloitte_worked") or "no",
        "deloitte_old_office": profile.get("deloitte_old_office") or "",
        "deloitte_old_email": profile.get("deloitte_old_email") or "",
        "deloitte_country": profile.get("deloitte_country") or "",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Récupère le profil Firestore (format extension)")
    parser.add_argument("--email", type=str, help="Email Firebase Auth (pour résoudre l'UID)")
    parser.add_argument("--uid", type=str, help="UID Firebase (prioritaire sur --email)")
    parser.add_argument("--key", type=str, help="Chemin vers le fichier JSON de la clé de compte de service")
    parser.add_argument("--bank", type=str, default="deloitte", help="Banque pour career_connections: deloitte, credit_agricole, etc. (default: deloitte)")
    parser.add_argument("--raw", action="store_true", help="Afficher aussi le document Firestore brut")
    args = parser.parse_args()

    if not args.uid and not args.email:
        print("Indiquez --email ou --uid.", file=sys.stderr)
        sys.exit(1)

    key_path = args.key or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not key_path or not os.path.isfile(key_path):
        print(
            "Clé de compte de service introuvable. Définir GOOGLE_APPLICATION_CREDENTIALS ou utiliser --key.",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        import firebase_admin
        from firebase_admin import credentials, firestore, auth
    except ImportError:
        print("Installer firebase-admin: pip install -r PYTHON/requirements_firebase.txt", file=sys.stderr)
        sys.exit(1)

    if not firebase_admin._apps:
        firebase_admin.initialize_app(credentials.Certificate(key_path))

    db = firestore.client()

    uid = args.uid
    if not uid and args.email:
        try:
            user = auth.get_user_by_email(args.email)
            uid = user.uid
            print(f"UID résolu pour {args.email}: {uid}", file=sys.stderr)
        except Exception as e:
            print(f"Impossible de trouver l'utilisateur pour {args.email}: {e}", file=sys.stderr)
            sys.exit(1)

    profile_ref = db.collection("profiles").document(uid)
    profile_snap = profile_ref.get()
    if not profile_snap.exists:
        print(f"Profil profiles/{uid} introuvable.", file=sys.stderr)
        sys.exit(1)

    profile_doc = profile_snap.to_dict()

    creds_doc = None
    conn_ref = profile_ref.collection("career_connections").document(args.bank)
    conn_snap = conn_ref.get()
    if conn_snap.exists:
        creds_doc = conn_snap.to_dict()
    else:
        for doc in profile_ref.collection("career_connections").stream():
            data = doc.to_dict()
            if (data.get("bankId") or "").lower() == args.bank.lower():
                creds_doc = data
                break

    normalized = normalize_profile(profile_doc, creds_doc, args.bank)

    def _serialize(obj):
        """Convert Firestore types (datetime, etc.) to JSON-serializable."""
        if hasattr(obj, "isoformat"):  # datetime, DatetimeWithNanoseconds
            return obj.isoformat()
        if isinstance(obj, dict):
            return {k: _serialize(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [_serialize(v) for v in obj]
        return obj

    out = {"uid": uid, "normalized_profile": normalized}
    if args.raw:
        out["firestore_profile"] = _serialize(profile_doc)
        out["firestore_career_connection"] = _serialize(creds_doc) if creds_doc else None

    out = _serialize(out)
    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
