import json
from collections import Counter
from pathlib import Path

import firebase_admin
from firebase_admin import auth, credentials, firestore

BASE_DIR = Path(__file__).resolve().parent
SERVICE_ACCOUNT_PATH = BASE_DIR / "firebase_service_Sccount_key.json"
DEFAULT_COLLECTION = "users"
DEFAULT_LIMIT = 5


class FirebaseAgent:

    def __init__(self, service_account_path: Path = SERVICE_ACCOUNT_PATH):

        self.service_account_path = service_account_path
        self.firebase_app = None
        self._owns_firebase_app = False
        self.firestore_client = self._create_firestore_client()


    def _create_firestore_client(self):

        try:
            self.firebase_app = firebase_admin.get_app()
        except ValueError:
            if not self.service_account_path.exists():
                raise FileNotFoundError(
                    f"Missing service account file: {self.service_account_path}"
                )

            cred = credentials.Certificate(str(self.service_account_path))
            self.firebase_app = firebase_admin.initialize_app(cred)
            self._owns_firebase_app = True

        return firestore.client(app=self.firebase_app)


    def close(self):

        if self.firestore_client is None:
            return

        try:
            close_client = getattr(self.firestore_client, "close", None)
            if callable(close_client):
                close_client()
            else:
                firestore_api = getattr(self.firestore_client, "_firestore_api", None)
                transport = getattr(firestore_api, "transport", None)
                close_transport = getattr(transport, "close", None)
                if callable(close_transport):
                    close_transport()
        finally:
            self.firestore_client = None

            if self._owns_firebase_app and self.firebase_app is not None:
                firebase_admin.delete_app(self.firebase_app)
                self.firebase_app = None
                self._owns_firebase_app = False


    def get_firebase_auth_users(self):

        users = []
        # The 'auth' is initialized by firebase_admin.initialize_app(...)
        page = auth.list_users()

        while page:
            for user in page.users:
                users.append(
                    {
                        "uid": user.uid,
                        "email": user.email,
                        "display_name": user.display_name,
                        "disabled": user.disabled,
                        "email_verified": user.email_verified,
                    }
                )
            page = page.get_next_page()

        return users
 

    def get_firestore_users_collection(self, collection_name: str = DEFAULT_COLLECTION, limit: int = DEFAULT_LIMIT):

        query = self.firestore_client.collection(collection_name)

        if limit is not None:
            query = query.limit(limit)

        docs = query.stream()
        return [{"id": doc.id, "data": doc.to_dict()} for doc in docs]


    def _iter_firestore_user_subscriptions(self, collection_name: str = DEFAULT_COLLECTION, limit: int = None):

        firestore_users = self.get_firestore_users_collection(
            collection_name=collection_name,
            limit=limit,
        )

        for firestore_user in firestore_users:
            firestore_data = firestore_user.get("data") or {}
            subscriptions = firestore_data.get("subscriptions") or []

            for subscription in subscriptions:
                if not isinstance(subscription, dict):
                    continue
                yield firestore_user, firestore_data, subscription


    def get_firebase_auth_users_by_firestore_disease_name(self, disease_name: str, collection_name: str = DEFAULT_COLLECTION, limit: int = None):

        firestore_users = self.get_firestore_users_collection(
            collection_name=collection_name,
            limit=limit,
        )
        auth_users = self.get_firebase_auth_users()
        auth_users_by_uid = {user["uid"]: user for user in auth_users}
        matched_users = []

        for firestore_user in firestore_users:
            firestore_data = firestore_user.get("data") or {}
            subscriptions = firestore_data.get("subscriptions") or []

            disease_names = {
                subscription.get("diseaseName")
                for subscription in subscriptions
                if isinstance(subscription, dict) and subscription.get("diseaseName")
            }

            if disease_name not in disease_names:
                continue

            uid = firestore_data.get("uid") or firestore_user.get("id")
            auth_user = auth_users_by_uid.get(uid)
            if not auth_user:
                continue

            matched_users.append(
                {
                    **auth_user,
                    "data": firestore_user["data"],
                }
            )

        return matched_users


    def get_firebase_auth_users_by_firestore_gard_id(self, gard_id: str, collection_name: str = DEFAULT_COLLECTION, limit: int = None):

        auth_users = self.get_firebase_auth_users()
        auth_users_by_uid = {user["uid"]: user for user in auth_users}
        matched_users = []
        matched_uids = set()

        for firestore_user, firestore_data, subscription in self._iter_firestore_user_subscriptions(
            collection_name=collection_name,
            limit=limit,
        ):
            if subscription.get("gardID") != gard_id:
                continue

            uid = firestore_data.get("uid") or firestore_user.get("id")
            if not uid or uid in matched_uids:
                continue

            auth_user = auth_users_by_uid.get(uid)
            if not auth_user:
                continue

            matched_uids.add(uid)
            matched_users.append(
                {
                    **auth_user,
                    "data": firestore_user["data"],
                }
            )

        return matched_users


    def get_firebase_authed_users_with_firestore_gard_ids_list(self, collection_name: str = DEFAULT_COLLECTION, limit: int = None):

        auth_users = self.get_firebase_auth_users()
        auth_users_by_uid = {
            user["uid"]: user
            for user in auth_users
            if not user.get("disabled") and user.get("email_verified")
        }

        matched_users = []

        firestore_users = self.get_firestore_users_collection(collection_name=collection_name, limit=limit,)

        for firestore_user in firestore_users:

            firestore_data = firestore_user.get("data") or {}
            uid = firestore_data.get("uid") or firestore_user.get("id")
            auth_user = auth_users_by_uid.get(uid)

            if not auth_user:
                continue

            subscriptions = firestore_data.get("subscriptions") or []

            gard_id_list = []
            subscription_map = {}
            for subscription in subscriptions:
                
                if not isinstance(subscription, dict):
                    continue

                gard_id = subscription.get("gardID")
                disease_name = subscription.get("diseaseName")

                if gard_id and gard_id not in gard_id_list:
                    gard_id_list.append(gard_id)

                if gard_id and disease_name:
                    subscription_map[gard_id] = disease_name

            matched_users.append(
                {
                    "display_name": auth_user.get("display_name"),
                    "email": auth_user.get("email"),
                    "gard_id_list": gard_id_list,
                    "subscriptions": subscription_map,
                }
            )

        return matched_users


    def get_firestore_registered_disease_names(self, collection_name: str = DEFAULT_COLLECTION, limit: int = None):

        firestore_users = self.get_firestore_users_collection(
            collection_name=collection_name,
            limit=limit,
        )
        disease_names = set()

        for firestore_user in firestore_users:
            firestore_data = firestore_user.get("data") or {}
            subscriptions = firestore_data.get("subscriptions") or []

            for subscription in subscriptions:
                if not isinstance(subscription, dict):
                    continue

                disease_name = subscription.get("diseaseName")
                if disease_name:
                    disease_names.add(disease_name)

        return sorted(disease_names)


    def get_firestore_registered_disease_names_with_counts(self, collection_name: str = DEFAULT_COLLECTION):

        disease_counter = Counter()

        for _, _, subscription in self._iter_firestore_user_subscriptions(
            collection_name=collection_name,
            limit=None,
        ):
            disease_name = subscription.get("diseaseName")
            if disease_name:
                disease_counter[disease_name] += 1

        return [
            {"diseaseName": disease_name, "count": count}
            for disease_name, count in disease_counter.most_common()
        ]


if __name__ == "__main__":
    #main()
    agent = FirebaseAgent()
    
    matched_users = agent.get_firebase_auth_users_by_firestore_disease_name('Chronic myeloid leukemia')
    print(json.dumps(matched_users, indent=2, ensure_ascii=False))

    disease_names = agent.get_firestore_registered_disease_names()
    print(json.dumps(disease_names, indent=2))

    rows = agent.get_firestore_registered_disease_names_with_counts()
    print(json.dumps(rows, indent=2))   

    users = agent.get_firebase_auth_users_by_firestore_gard_id('GARD:0017890')
    print(json.dumps(users, indent=2, ensure_ascii=False))

    sers = agent.get_firebase_authed_users_with_firestore_gard_ids_list()
    print(json.dumps(sers, indent=2, ensure_ascii=False))
    
