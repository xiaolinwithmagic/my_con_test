# world_state.py
import logging

logging.basicConfig(level=logging.DEBUG)

class WorldState:
    def __init__(self):
        # image_hash -> {"owner": did, "licensees": [ {...} ], "history": [txs...] }
        self.state = {}

    def get_owner(self, image_hash):
        rec = self.state.get(image_hash)
        return rec["owner"] if rec else None

    def apply_transaction(self, tx: dict):
        ttype = tx["type"]
        ih = tx["image_hash"]
        if ttype == "REGISTER":
            # if already registered, ignore (or optionally record double register)
            if ih not in self.state:
                self.state[ih] = {"owner": tx["creator_did"], "licensees": [], "history": [tx]}
                return True
            else:
                # already registered -> reject or record
                self.state[ih]["history"].append(tx)
                return False
        elif ttype == "TRANSFER":
            if ih in self.state and self.state[ih]["owner"] == tx["from_did"]:
                self.state[ih]["owner"] = tx["to_did"]
                self.state[ih]["history"].append(tx)
                return True
            else:
                # invalid transfer
                self.state.setdefault(ih, {"owner": None, "licensees": [], "history": []})["history"].append(tx)
                return False
        elif ttype == "LICENSE":
            if ih in self.state and self.state[ih]["owner"] == tx["owner_did"]:
                self.state[ih]["licensees"].append({"licensee": tx["licensee_did"], "terms": tx.get("terms")})
                self.state[ih]["history"].append(tx)
                return True
            else:
                self.state.setdefault(ih, {"owner": None, "licensees": [], "history": []})["history"].append(tx)
                return False
        else:
            # unknown tx type
            return False
