#!/usr/bin/env python3
"""
seed_demo_account.py — create/refresh the sales-demo client with synthetic leads.

Builds "Really Great Personal Injury Attorneys": ~50 generated leads over the
past 6 weeks (answered calls, missed calls, voicemails, message threads, and a
handful of spam examples), daily ad-impression metrics, and a 30-day summary at
~$85/lead. All names and numbers are fictional (555-01xx range). The client is
flagged is_demo so syncs, scrapes, and phone lookups never touch it.

Re-runnable: deletes any existing client with the demo slug, then recreates it.

Usage:
    source .venv/bin/activate
    PYTHONPATH=. python scripts/seed_demo_account.py
"""

import asyncio
import json
import random
from datetime import datetime, timedelta
from pathlib import Path

import bcrypt
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

import app.database as db                      # noqa: E402
from app.r2 import upload_audio, _enabled as r2_enabled   # noqa: E402

SLUG = "really-great-personal-injury"
NAME = "Really Great Personal Injury Attorneys"
PORTAL_PASSWORD = "demo2026"
BASE = "https://lsa.tripletakemarketing.com"
LEAD_LIST_URL = f"{BASE}/static/demo/lsa-lead-list.html"
LEAD_DETAIL_URL = f"{BASE}/static/demo/lsa-lead-detail.html"
AUDIO_R2_KEY = "demo/sample-lead-recording.mp3"
COST_PER_LEAD = 85.0

rng = random.Random(20260720)

FIRST = ["James", "Maria", "Robert", "Linda", "Michael", "Patricia", "David", "Jennifer",
         "William", "Elizabeth", "Richard", "Barbara", "Joseph", "Susan", "Thomas",
         "Jessica", "Charles", "Sarah", "Christopher", "Karen", "Daniel", "Nancy",
         "Matthew", "Lisa", "Anthony", "Betty", "Mark", "Margaret", "Donald", "Sandra"]
LAST = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
        "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
        "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson"]
CITIES = ["Atlanta", "Marietta", "Decatur", "Smyrna", "Roswell", "Sandy Springs",
          "Alpharetta", "Duluth", "Lawrenceville", "Douglasville", "Kennesaw", "Austell"]
AREA_CODES = ["404", "470", "678", "770"]

_seq = [0]


def fake_phone() -> str:
    _seq[0] += 1
    return f"({rng.choice(AREA_CODES)}) 555-{100 + _seq[0]:04d}"


def fake_name() -> str:
    return f"{rng.choice(FIRST)} {rng.choice(LAST)}"


# ── Scenario templates ────────────────────────────────────────────────────────

CALL_SCENARIOS = [
    ("Auto accidents",
     "I was rear-ended on I-285 {when} and my neck has been killing me ever since. The other driver's insurance keeps lowballing me.",
     "requesting help with a rear-end collision injury claim", 5),
    ("Auto accidents",
     "Someone ran a red light and T-boned my car {when}. My arm is fractured and I've been out of work for two weeks.",
     "seeking representation for an intersection collision with injuries", 5),
    ("Slip & fall",
     "I slipped on a wet floor at the grocery store {when} — no warning sign anywhere. I hurt my back and have medical bills piling up.",
     "premises liability claim for a grocery store slip and fall", 4),
    ("Workers' compensation",
     "I injured my shoulder lifting pallets at the warehouse {when}. My employer is pressuring me not to file a claim.",
     "workers' compensation claim for a workplace shoulder injury", 4),
    ("Dog bites",
     "My neighbor's dog bit my daughter {when}. She needed stitches and we have the medical records.",
     "dog bite injury claim involving a minor", 5),
    ("Motorcycle accidents",
     "A pickup merged into my lane and knocked me off my motorcycle {when}. Road rash and a broken wrist.",
     "motorcycle accident with documented injuries", 4),
    ("Truck accidents",
     "A delivery truck sideswiped me on the connector {when}. Their insurance already called me twice — I don't want to say the wrong thing.",
     "commercial truck accident, caller needs guidance before insurer contact", 5),
    ("Auto accidents",
     "I was a passenger in an Uber that got hit {when}. Not sure whose insurance is responsible and nobody is calling me back.",
     "rideshare passenger injury claim", 4),
    ("Pedestrian accidents",
     "I was hit in a crosswalk by a driver making a turn {when}. Ambulance took me in; I have a sprained knee and a concussion.",
     "pedestrian struck in a crosswalk with ER records", 5),
    ("Auto accidents",
     "Just wondering how much you charge to look at an accident case? It happened almost two years ago.",
     "price shopper with a case near the statute of limitations", 3),
]

MESSAGE_SCENARIOS = [
    ("Auto accidents", "I was in a car accident on {road} last week. Other driver cited. Neck and back pain, seen a chiropractor twice. Do I have a case?", 5),
    ("Slip & fall", "Fell in a restaurant parking lot — broken wrist. The property manager admitted the lighting was out. Looking for a consultation.", 4),
    ("Workers' compensation", "Hurt my knee on a construction site. Employer says I was a contractor, but I clocked in daily. Need advice on comp eligibility.", 4),
    ("Auto accidents", "Hit and run on {road}. Police have the plate. My car is totaled and my shoulder hurts. What are my next steps?", 5),
    ("Dog bites", "Bitten by a loose dog while jogging. Animal control has the report. Puncture wounds, urgent care visit. Wondering about compensation.", 4),
    ("Medical malpractice", "I think my surgery was botched — second surgeon says the first one made an error. How do malpractice cases work?", 3),
    ("Auto accidents", "My teenage son was in an accident driving my car. Insurance is confusing us. Free consult available?", 4),
    ("Slip & fall", "Slipped on ice outside my apartment building. Complex never salts the walkways — I have photos and texts to management.", 4),
]

ROADS = ["I-285", "I-75", "GA-400", "I-20", "Peachtree Rd", "the connector"]
WHENS = ["last week", "two weeks ago", "on Tuesday", "over the weekend", "a few days ago", "last month"]
REPS = ["Rachel", "Amanda", "Kevin", "Denise"]


def call_transcript(caller: str, story: str, city: str) -> str:
    rep = rng.choice(REPS)
    first = caller.split()[0]
    return (
        f"RECEPTIONIST: Thank you for calling {NAME}, this is {rep}. How can I help you today?\n"
        f"CALLER: Hi, yes. {story}\n"
        f"RECEPTIONIST: I'm so sorry to hear that — you've called the right place. Can I get your name and a good callback number?\n"
        f"CALLER: Sure, it's {caller}.\n"
        f"RECEPTIONIST: Thank you, {first}. And this happened in the {city} area?\n"
        f"CALLER: Yes, that's right.\n"
        f"RECEPTIONIST: Okay. One of our attorneys will call you back today for a free consultation. In the meantime, please don't give any recorded statements to the insurance company.\n"
        f"CALLER: That's great, thank you so much."
    )


def message_transcript(caller: str, inquiry: str, hour: int) -> str:
    t1 = f"{(hour % 12) or 12}:{rng.randrange(10, 59)} PM" if hour >= 12 else f"{hour}:{rng.randrange(10, 59)} AM"
    t2 = f"{((hour + 1) % 12) or 12}:{rng.randrange(10, 59)} PM" if hour + 1 >= 12 else f"{hour + 1}:{rng.randrange(10, 59)} AM"
    return (
        f"{caller} ({t1}): {inquiry}\n"
        f"{NAME} ({t2}): Thank you for reaching out — we're sorry to hear about your situation. "
        f"One of our attorneys will review your message and contact you today to schedule a free consultation.\n"
        f"{caller} ({t2}): Thank you, I'll be available all afternoon."
    )


def analysis_json(score, reason, summary, service, follow_up, spam=2, spam_type=None, name=None, answered=True):
    return json.dumps({
        "was_answered": answered, "contact_name": name,
        "qualification_score": score, "qualification_reason": reason,
        "call_summary": summary, "service_requested": service,
        "follow_up_required": follow_up,
        "follow_up_notes": "Attorney callback scheduled." if follow_up else None,
        "spam_likelihood": spam, "spam_type": spam_type,
    })


def stamp(dt: datetime) -> dict:
    iso = dt.isoformat(timespec="seconds")
    return {"scraped_at": iso, "transcribed_at": iso, "analyzed_at": iso}


# ── Lead builders ─────────────────────────────────────────────────────────────

def build_leads(now: datetime, audio_key: str | None) -> list[dict]:
    leads: list[dict] = []
    lead_id = [900000000]

    def nid() -> str:
        lead_id[0] += 1
        return str(lead_id[0])

    # Spread datetimes over the past 42 days, weekday-weighted. Build backwards
    # from today so the newest lead always lands on the current date — the demo
    # must look like an actively-running account.
    slots: list[datetime] = []
    back = 0
    while len(slots) < 50 and back < 42:
        d = now - timedelta(days=back)
        n = rng.choice([1, 1, 2, 2, 2, 3]) if d.weekday() < 5 else rng.choice([0, 1, 1])
        if back == 0:
            n = max(n, 1)          # guarantee at least one lead today
        for _ in range(n):
            hour = rng.randrange(8, min(now.hour + 1, 19)) if back == 0 and now.hour > 8 else rng.randrange(8, 19)
            slots.append(d.replace(hour=hour, minute=rng.randrange(0, 59), second=0, microsecond=0))
        back += 1
    slots = sorted(slots)[-50:]

    def base(dt, **kw) -> dict:
        d = {
            "id": nid(), "lead_type": "phone", "location": rng.choice(CITIES),
            "call_date": dt.isoformat(timespec="seconds"), "lead_url": LEAD_DETAIL_URL,
            "scrape_status": "completed", "transcription_status": "completed",
            "analysis_status": "completed", **stamp(dt),
        }
        d.update(kw)
        return d

    i = 0
    for dt in slots:
        kind = i % 10  # deterministic mix over the sequence
        caller, phone, city = fake_name(), fake_phone(), rng.choice(CITIES)
        if kind in (0, 1, 2, 3):        # answered phone lead (good)
            job, story_t, svc, score = CALL_SCENARIOS[i % len(CALL_SCENARIOS)]
            story = story_t.format(when=rng.choice(WHENS))
            summary = (f"{caller} called about {svc}. The receptionist collected contact details "
                       f"and scheduled a free attorney consultation for the same day.")
            reason = f"Genuine {job.lower()} inquiry with clear injury details and intent to hire."
            leads.append(base(dt, caller_phone=phone, contact_name=caller, job_type=job,
                              charge_status="Charged" if kind != 3 else "In review",
                              call_duration_seconds=rng.randrange(95, 420), is_answered=1,
                              audio_url=audio_key,
                              transcript=call_transcript(caller, story, city),
                              qualification_score=score,
                              qualification_reason=reason, call_summary=summary,
                              analysis_json=analysis_json(score, reason, summary, job, True, name=caller),
                              spam_score=rng.randrange(0, 12), spam_reasons=None))
        elif kind == 4:                 # message lead
            job, inquiry_t, score = MESSAGE_SCENARIOS[i % len(MESSAGE_SCENARIOS)]
            inquiry = inquiry_t.format(road=rng.choice(ROADS))
            summary = (f"{caller} sent a message about {job.lower()}. The office replied and "
                       f"an attorney consultation was arranged.")
            reason = "Detailed written inquiry describing injuries and requesting a consultation."
            leads.append(base(dt, lead_type="message", caller_name=caller, contact_name=caller,
                              job_type=job, charge_status="Charged", is_answered=1,
                              transcript=message_transcript(caller, inquiry, dt.hour),
                              qualification_score=score, qualification_reason=reason,
                              call_summary=summary,
                              analysis_json=analysis_json(score, reason, summary, job, True, name=caller),
                              spam_score=rng.randrange(0, 10), spam_reasons=None))
        elif kind == 5:                 # missed call, no recording
            leads.append(base(dt, caller_phone=phone, job_type=None,
                              charge_status=rng.choice(["Not charged", "Charged"]),
                              call_duration_seconds=rng.randrange(4, 20), is_answered=0,
                              call_summary="Missed call — no recording available.",
                              analysis_json=analysis_json(None, None,
                                  "Missed call — no recording available.", None, False, answered=False),
                              spam_score=0, spam_reasons=None))
        elif kind == 6:                 # voicemail with recording
            job = "Auto accidents"
            vm = (f"RECEPTIONIST: You've reached {NAME}. Please leave your name and number and we "
                  f"will return your call promptly.\n"
                  f"CALLER: Hi, my name is {caller}. I was in a car accident on {rng.choice(ROADS)} "
                  f"and I'd like to talk to an attorney about it. My number is {phone}. "
                  f"Please call me back when you can. Thank you.")
            summary = f"{caller} left a voicemail requesting a callback about a car accident claim."
            reason = "Voicemail with a clear case inquiry and callback number — promising lead."
            leads.append(base(dt, caller_phone=phone, contact_name=caller, job_type=job,
                              charge_status="Charged", call_duration_seconds=rng.randrange(35, 90),
                              is_answered=0, audio_url=audio_key, transcript=vm,
                              qualification_score=3, qualification_reason=reason, call_summary=summary,
                              analysis_json=analysis_json(3, reason, summary, job, True,
                                                          name=caller, answered=False),
                              spam_score=rng.randrange(0, 10), spam_reasons=None))
        elif kind == 7:                 # short hang-up
            tr = (f"RECEPTIONIST: Thank you for calling {NAME}, this is {rng.choice(REPS)}. "
                  f"Hello? ... Hello?\nCALLER: (no response)")
            summary = "The caller did not speak and the call ended after a few seconds."
            reason = "No conversation took place — possible misdial."
            leads.append(base(dt, caller_phone=phone, job_type=None, charge_status="Not charged",
                              call_duration_seconds=rng.randrange(5, 14), is_answered=1,
                              audio_url=audio_key, transcript=tr, qualification_score=1,
                              qualification_reason=reason, call_summary=summary,
                              analysis_json=analysis_json(1, reason, summary, None, False, spam=25),
                              spam_score=25, spam_reasons=None))
        elif kind == 8:                 # another message lead
            job, inquiry_t, score = MESSAGE_SCENARIOS[(i + 3) % len(MESSAGE_SCENARIOS)]
            inquiry = inquiry_t.format(road=rng.choice(ROADS))
            summary = f"{caller} inquired about {job.lower()} via message; consultation offered."
            reason = "Legitimate written inquiry with case details."
            leads.append(base(dt, lead_type="message", caller_name=caller, contact_name=caller,
                              job_type=job, charge_status="Charged", is_answered=1,
                              transcript=message_transcript(caller, inquiry, dt.hour),
                              qualification_score=score, qualification_reason=reason,
                              call_summary=summary,
                              analysis_json=analysis_json(score, reason, summary, job, True, name=caller),
                              spam_score=rng.randrange(0, 10), spam_reasons=None))
        else:                            # kind == 9: rotate through spam examples
            leads.append(_spam_lead(base, dt, i, audio_key))
        i += 1
    return leads


def _spam_lead(base, dt, i, audio_key) -> dict:
    variant = (i // 10) % 5
    phone = fake_phone()
    if variant == 0:      # SEO solicitor call — charged (dispute candidate)
        tr = (f"RECEPTIONIST: Thank you for calling {NAME}, how can I help you?\n"
              f"CALLER: Hi, I'm with a digital marketing agency. We noticed your firm isn't ranking "
              f"on page one of Google. We guarantee first-page results for law firms.\n"
              f"RECEPTIONIST: We're not interested in marketing services, thank you.\n"
              f"CALLER: This is a limited-time offer — can I speak with the partner in charge?\n"
              f"RECEPTIONIST: Please remove us from your list. Goodbye.")
        summary = "Telemarketer selling SEO services to the firm — not a client inquiry."
        return base(dt, caller_phone=phone, job_type=None, charge_status="Charged",
                    call_duration_seconds=88, is_answered=1, audio_url=audio_key, transcript=tr,
                    qualification_score=1,
                    qualification_reason="Solicitor selling marketing services, not a potential client.",
                    call_summary=summary,
                    analysis_json=analysis_json(1, "Solicitation call.", summary, None, False,
                                                spam=88, spam_type="solicitor"),
                    spam_score=88, spam_reasons="AI: solicitor (88%)")
    if variant == 1:      # robocall — credited by Google
        tr = ("RECEPTIONIST: Thank you for calling, how can I help you?\n"
              "CALLER: (recorded message) This is an important announcement about your vehicle's "
              "extended warranty. Press one to speak with a representative...")
        summary = "Automated robocall about vehicle warranties — no human on the line."
        return base(dt, caller_phone=phone, job_type=None, charge_status="Credited",
                    call_duration_seconds=41, is_answered=1, audio_url=audio_key, transcript=tr,
                    qualification_score=1, qualification_reason="Prerecorded robocall.",
                    call_summary=summary,
                    analysis_json=analysis_json(1, "Robocall.", summary, None, False,
                                                spam=93, spam_type="robocall"),
                    spam_score=93, spam_reasons="AI: robocall (93%)")
    if variant == 2:      # IPQS-flagged number
        tr = (f"RECEPTIONIST: Thank you for calling {NAME}.\n"
              f"CALLER: Yeah, is this the law office? ... Actually never mind. (click)")
        summary = "Brief call from a number with a poor reputation; caller hung up quickly."
        return base(dt, caller_phone=phone, job_type=None, charge_status="Not charged",
                    call_duration_seconds=19, is_answered=1, audio_url=audio_key, transcript=tr,
                    qualification_score=1, qualification_reason="No inquiry; high-risk caller number.",
                    call_summary=summary,
                    analysis_json=analysis_json(1, "No inquiry.", summary, None, False, spam=35),
                    spam_score=75, spam_reasons="number flagged for abuse/spam (IPQS)",
                    phone_lookup_json=json.dumps({"provider": "ipqs", "fraud_score": 91,
                                                  "recent_abuse": True, "spammer": True,
                                                  "line_type": "VOIP", "carrier": "Sample VoIP Co"}))
    if variant == 3:      # cross-account repeat caller
        tr = (f"RECEPTIONIST: Thank you for calling {NAME}.\n"
              f"CALLER: Hello, we buy unwanted legal case leads in bulk. Are you the owner?\n"
              f"RECEPTIONIST: Not interested, thank you. (click)")
        summary = "Lead-reseller solicitation; the same caller has contacted several of our accounts."
        return base(dt, caller_phone=phone, job_type=None, charge_status="Not charged",
                    call_duration_seconds=33, is_answered=1, audio_url=audio_key, transcript=tr,
                    qualification_score=1, qualification_reason="Solicitor targeting multiple businesses.",
                    call_summary=summary,
                    analysis_json=analysis_json(1, "Solicitation.", summary, None, False,
                                                spam=70, spam_type="solicitor"),
                    spam_score=95, spam_reasons="AI: solicitor (70%); same caller seen on 2 other account(s)")
    # variant 4: suspicious-but-uncertain (amber) message
    caller = fake_name()
    tr = (f"{caller} (11:02 AM): Hello dear, I have amazing business proposal for legal firm, "
          f"very profitable, please reply WhatsApp for details.\n"
          f"{NAME} (11:20 AM): This appears to be unsolicited marketing. We are not interested.")
    summary = "Unsolicited business-proposal message; likely spam but phrased as an inquiry."
    return base(dt, lead_type="message", caller_name=caller, job_type=None,
                charge_status="In review", is_answered=1, transcript=tr,
                qualification_score=1, qualification_reason="Unsolicited proposal, not a case inquiry.",
                call_summary=summary,
                analysis_json=analysis_json(1, "Unsolicited proposal.", summary, None, False,
                                            spam=55, spam_type="solicitor"),
                spam_score=55, spam_reasons="AI: solicitor (55%)")


# ── Main ──────────────────────────────────────────────────────────────────────

async def main() -> None:
    await db.init_db()
    now = datetime.now().replace(microsecond=0)

    # 1) fresh demo client
    existing = await db.get_client_by_slug(SLUG)
    if existing:
        await db.delete_client(existing["id"])
        print(f"Deleted existing demo client (id {existing['id']}).")
    pw_hash = bcrypt.hashpw(PORTAL_PASSWORD.encode(), bcrypt.gensalt()).decode()
    client = await db.create_client(NAME, SLUG, LEAD_LIST_URL, pw_hash)
    cid = client["id"]

    # 2) demo audio — synthesize once and park it in R2
    audio_key = None
    if r2_enabled():
        try:
            from openai import OpenAI
            speech = OpenAI().audio.speech.create(
                model="tts-1", voice="alloy",
                input="This is a sample lead recording, provided for demonstration purposes.")
            tmp = Path("/tmp/demo-lead-recording.mp3")
            tmp.write_bytes(speech.content)
            if await upload_audio(str(tmp), AUDIO_R2_KEY):
                audio_key = AUDIO_R2_KEY
            tmp.unlink(missing_ok=True)
        except Exception as e:
            print(f"WARN: demo audio generation failed ({e}) — leads will have no recording.")

    # 3) leads
    leads = build_leads(now, audio_key)
    for lead in leads:
        await db.upsert_lead(cid, lead)
    print(f"Inserted {len(leads)} synthetic leads.")

    # 4) daily ad impressions for the chart
    for d in range(42):
        day = (now - timedelta(days=d)).date()
        base_val = 70 if day.weekday() < 5 else 45
        await db.upsert_daily_metric(cid, day.isoformat(), base_val + rng.randrange(-25, 45))

    # 5) client metadata: 30-day summary at ~$85/lead + demo flags
    cutoff = (now - timedelta(days=30)).isoformat(timespec="seconds")
    charged = [l for l in leads
               if l["call_date"] >= cutoff and (l.get("charge_status") or "").startswith("Charged")]
    spend = round(len(charged) * COST_PER_LEAD + rng.uniform(-40, 40), 2)
    await db.update_client(cid, {
        "is_demo": 1,
        "business_type": "personal injury law firm",
        "google_account_id": "5550000000",
        "portal_password_plain": PORTAL_PASSWORD,
        "r30_leads": len(charged),
        "r30_spend": spend,
        "r30_updated_at": now.isoformat(timespec="seconds"),
        "last_synced_at": now.isoformat(timespec="seconds"),
        "last_sync_new_leads": 2,
    })
    print(f"Demo client id {cid} ready. 30-day: {len(charged)} leads / ${spend:,.2f} "
          f"(avg ${spend / max(1, len(charged)):,.2f}).")
    print(f"Portal: {BASE}/portal/{SLUG}   password: {PORTAL_PASSWORD}")
    await db.close_db()


if __name__ == "__main__":
    asyncio.run(main())
