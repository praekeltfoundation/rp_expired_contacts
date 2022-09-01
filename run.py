#!/usr/bin/env python
import json
import os
from datetime import datetime
from urllib.parse import urljoin

import requests
from temba_client.v2 import TembaClient

RAPIDPRO_URL = os.environ["RAPIDPRO_URL"]
RAPIDPRO_TOKEN = os.environ["RAPIDPRO_TOKEN"]
TURN_URL = os.environ["TURN_URL"]
TURN_TOKEN = os.environ["TURN_TOKEN"]

rapidpro = TembaClient(RAPIDPRO_URL, RAPIDPRO_TOKEN)


def archive_turn_conversation(urn, message_id, reason):
    headers = {
        "Authorization": f"Bearer {TURN_TOKEN}",
        "Accept": "application/vnd.v1+json",
        "Content-Type": "application/json",
    }

    data = json.dumps({"before": message_id, "reason": reason})

    r = requests.post(
        urljoin(TURN_URL, f"v1/chats/{urn}/archive"),
        headers=headers,
        data=data,
    )
    r.raise_for_status()


def update_rapidpro_contact(urn, fields):
    rapidpro.update_contact(urn, fields=fields)


batches = rapidpro.get_contacts().iterfetches(retry_on_rate_exceed=True)
for contact_batch in batches:
    for contact in contact_batch:
        if (
            contact.fields.get("wait_for_helpdesk")
            and contact.fields.get("wait_for_helpdesk") == "True"
            and contact.fields.get("before")
        ):
            fields = {"helpdesk_flag": str(datetime.now())}
            rapidpro.update_contact(contact=contact.uuid, fields=fields)
            flag = datetime.strptime(
                contact.fields["helpdesk_flag"], "%Y-%m-%dT%H:%M:%S.%f+02:00"
            ).date()
            today = datetime.strptime(
                str(datetime.now()), "%Y-%m-%d %H:%M:%S.%f"
            ).date()
            delta = today - flag
            if delta.days > 5:
                print(contact.fields)
                wa_id = None
                for urn in contact.urns:
                    if "whatsapp" in urn:
                        wa_id = urn.split(":")[1]

                if wa_id:
                    archive_turn_conversation(
                        wa_id,
                        contact.fields["before"],
                        f"Auto archived after {delta.days} days",
                    )
                update_rapidpro_contact(
                    contact.uuid,
                    {
                        "helpdesk_flag": None,
                        "wait_for_helpdesk": None,
                    },
                )
                print("Archived contact")
