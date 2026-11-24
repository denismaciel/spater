"""Wiring for the sample jobs. The business modules contain plain Python."""

from __future__ import annotations

from datetime import timedelta as td

from spater.flow import Builder, Context, Job, PerStep, PerWorker, Ref, Retry

from . import loss_runs as lr
from . import onboarding as ob
from . import quotes as q
from . import reports as rp
from . import triage as tr
from .mail import Email, open_db, open_mailer


def report_write(ctx: Context) -> rp.ReportWrite:
    return rp.ReportWrite(ctx.idempotency_key)


MAILER = PerWorker(open_mailer)
DB = PerWorker(open_db)


# A retried write uses the same key. The worker supplies the database handle.
@Job.define("nightly_report", provides=[DB, PerStep(report_write)])
def NIGHTLY(f: Builder, day: Ref[rp.Day]) -> Ref[rp.Report]:
    rows = f.step(rp.extract, day, timeout=td(minutes=10))
    report = f.step(rp.transform, day, rows)
    f.step(rp.load, report)
    return report


def _handle_silence(b: Builder, policy: Ref[lr.Policy], pending: Ref[lr.Pending]) -> Ref[lr.Received]:
    decision = b.step(lr.on_silence, pending)
    return b.branch(
        decision,
        {
            lr.Remind: lambda r, _: _remind_and_recheck(r, pending),
            lr.GiveUp: lambda r, _: r.fail("carrier never replied"),
        },
    )


def _remind_and_recheck(b: Builder, pending: Ref[lr.Pending]) -> Ref[lr.Received]:
    reminded = b.step(lr.send_reminder, pending)
    waited = b.sleep(td(days=2), name="wait_after_reminder", after=reminded)
    recheck = b.step(lr.check_reply, pending, name="recheck_reply", after=waited)
    return b.branch(
        recheck,
        {
            lr.Received: lambda _, received: received,
            lr.NoReply: lambda r, _: r.fail("carrier never replied"),
        },
        name="branch_recheck",
    )


# Sleep until the request window, send, wait for the reply window, remind once if silent.
@Job.define("loss_runs", provides=[MAILER, DB])
def LOSS_RUNS(f: Builder, policy: Ref[lr.Policy]) -> Ref[lr.LossRuns]:
    window = f.sleep(lr.until_request_window, policy, name="wait_request_window")
    pending = f.step(lr.send_request, policy, timeout=td(seconds=30), after=window)
    waited = f.sleep(lr.reply_window, policy, name="wait_reply_window", after=pending)
    status = f.step(lr.check_reply, pending, after=waited)
    received = f.branch(
        status,
        {
            lr.Received: lambda _, rec: rec,
            lr.NoReply: lambda b, _: _handle_silence(b, policy, pending),
        },
    )
    runs = f.step(lr.parse, received, pending)
    f.step(lr.store, runs)
    return runs


# Retry transient timeouts up to 3 attempts, then convert to NoQuote if still timing out.
@Job.define("carrier_quote", provides=[MAILER])
def CARRIER_QUOTE(f: Builder, ask: Ref[q.CarrierAsk]) -> Ref[q.Quote | q.NoQuote]:
    asked = f.step(
        q.send_ask,
        ask,
        retry=Retry(on=TimeoutError, max=3, after=td(seconds=2), backoff=2.0),
        catch={TimeoutError: q.carrier_unavailable},
    )
    return f.branch(
        asked,
        {
            q.NoQuote: lambda _, no_quote: no_quote,
            q.PendingAsk: lambda b, pending: b.step(q.fetch_quote, pending),
        },
    )


# Validate, fan out across carriers, compare results, and notify the broker.
@Job.define("submission", provides=[MAILER])
def SUBMISSION(f: Builder, sub: Ref[q.Submission]) -> Ref[q.Recommendation]:
    validated = f.step(q.validate, sub)
    asks = f.step(q.carriers, sub, after=validated)
    quotes = f.map(CARRIER_QUOTE, asks, max_parallel=10)
    rec = f.step(q.compare, sub, quotes)
    f.step(q.notify_broker, rec)
    return rec


# create_mailbox, fetch_documents and credit_check have no mutual dependencies, so they run in parallel automatically.
@Job.define("onboard", provides=[MAILER])
def ONBOARD(f: Builder, account: Ref[ob.Account]) -> Ref[ob.Activation]:
    mailbox = f.step(ob.create_mailbox, account)
    documents = f.step(ob.fetch_documents, account)
    score = f.step(ob.credit_check, account)
    outcome = f.step(ob.activate, account, mailbox, documents, score)
    return f.branch(
        outcome,
        {
            ob.Activation: lambda _, activation: activation,
            ob.CreditDeclined: lambda b, _: b.fail("credit score below minimum"),
            ob.MissingDocuments: lambda b, _: b.fail("no documents supplied"),
        },
    )


def _handle_claim(b: Builder, email: Ref[Email], claim: Ref[tr.Claim]) -> Ref[tr.Draft]:
    claim_ref = b.step(tr.open_claim, claim)
    return b.step(tr.draft_ack, email, claim_ref)


# Each surviving arm returns a Draft; spam fails immediately.
@Job.define("triage", provides=[MAILER, DB])
def TRIAGE(f: Builder, email: Ref[Email]) -> None:
    outcome = f.step(tr.classify, email, timeout=td(minutes=1))
    draft = f.branch(
        outcome,
        {
            tr.Spam: lambda b, _: b.fail("spam"),
            tr.Question: lambda b, question: b.step(tr.draft_answer, email, question),
            tr.Claim: lambda b, claim: _handle_claim(b, email, claim),
        },
    )
    f.step(tr.send, draft)


# Both extractions return Rows and run concurrently; transform explicitly takes `archive`.
@Job.define("archived_report")
def ARCHIVED_REPORT(f: Builder, day: Ref[rp.Day]) -> Ref[rp.Report]:
    f.step(rp.extract, day)
    archive = f.step(rp.extract_archive, day)
    return f.step(rp.transform, day, archive)


ALL = [NIGHTLY, LOSS_RUNS, CARRIER_QUOTE, SUBMISSION, ONBOARD, TRIAGE, ARCHIVED_REPORT]
