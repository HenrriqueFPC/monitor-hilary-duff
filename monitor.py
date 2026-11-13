#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Monitor de nova data / camarote - Hilary Duff (Ticketmaster Brasil).

O que ele vigia (so biblioteca padrao do Python, nada para instalar):
  1. Pagina do evento  -> novo botao de data, nova data citada no texto, status mudando
  2. Paginas de cada data -> status de venda e disponibilidade do CAMAROTE (devolucao/reposicao)
  3. Enderecos "provaveis" de uma nova data (a pagina costuma existir antes de aparecer no site)
  4. Fila virtual (Queue-it) com nomes provaveis -> a fila e criada ANTES da venda abrir
  5. Home da Ticketmaster e pagina da artista na Live Nation Brasil -> novos links
  6. Google News -> manchetes sobre nova data

Uso:
  python monitor.py            roda em loop (deixe aberto, ou use iniciar_monitor.bat)
  python monitor.py --once     roda uma rodada e sai
  python monitor.py --status   mostra o que ele esta enxergando agora
  python monitor.py --test-alert   dispara um alerta de teste
  python monitor.py --stop     encerra o monitor que estiver rodando em segundo plano
"""
import argparse
import ctypes
import gzip
import html
import json
import os
import random
import re
import socket
import subprocess
import sys
import threading
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

BASE = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(BASE, "state.json")
LOG_FILE = os.path.join(BASE, "monitor.log")
ALERT_FILE = os.path.join(BASE, "ALERTAS.txt")
CONFIG_FILE = os.path.join(BASE, "config.json")
PID_FILE = os.path.join(BASE, "monitor.pid")

TM = "https://www.ticketmaster.com.br"

# Qualquer chave daqui pode ser sobrescrita em config.json
DEFAULTS = {
    "landing_url": TM + "/event/hilary-duff",
    "artist_regex": r"hilary|duff",
    # setores que voce quer (alerta quando ficarem disponiveis em qualquer data)
    "watch_sectors": ["camarote"],
    # paginas de data ja conhecidas (novas sao descobertas sozinhas)
    "event_slugs": ["vg-hilary-duff", "vg-hilary-duff-data-extra"],
    # palpites de endereco para uma proxima data
    "candidate_slugs": [
        "vg-hilary-duff-data-extra-2", "vg-hilary-duff-data-extra2",
        "vg-hilary-duff-data-extra-ii", "vg-hilary-duff-data-extra-3",
        "vg-hilary-duff-extra-2", "vg-hilary-duff-2", "vg-hilary-duff-3",
        "vg-hilary-duff-nova-data", "vg-hilary-duff-nova-data-extra",
        "vg-hilary-duff-segunda-data-extra", "vg-hilary-duff-terceira-data",
        "vg-hilary-duff-data-extra-15",
    ],
    # palpites de nome de fila (a fila da data extra se chama "hilaryduffextra")
    "queue_base": "https://ticketmasterbr.queue-it.net/?c=ticketmasterbr&e=",
    "queue_candidates": [
        "hilaryduffextra2", "hilaryduffextra3", "hilaryduff2", "hilaryduff3",
        "hilaryduffdataextra2", "hilaryduffextraii", "hilaryduffnovadata",
        "hilaryduffextra02",
    ],
    "livenation_url": "https://www.livenation.com.br/hilary-duff-tickets-adp1632382",
    "news_feeds": [
        "https://news.google.com/rss/search?q=%22Hilary+Duff%22+(%22data+extra%22+OR+%22nova+data%22+OR+%22terceira+data%22+OR+%22novo+show%22+OR+%22terceiro+show%22)&hl=pt-BR&gl=BR&ceid=BR:pt-419",
        "https://news.google.com/rss/search?q=%22Hilary+Duff%22+%22S%C3%A3o+Paulo%22+when:7d&hl=pt-BR&gl=BR&ceid=BR:pt-419",
    ],
    # intervalos em segundos
    # "events" = paginas de cada data (camarote). Camarote devolvido some em ~2 min.
    # A Ticketmaster guarda a pagina em cache por 15 s, entao abaixo de ~5 s nao vem informacao nova.
    # "slugs" e "queue" testam UM palpite por vez (12 e 8 palpites -> cada um a cada ~10 min).
    # "x" checa UMA conta por vez: a produtora a cada ~1 min, as demais a cada ~4 min.
    "intervals": {"landing": 30, "events": 5, "slugs": 45, "home": 300,
                  "queue": 75, "livenation": 600, "news": 300, "x": 30},
    # contas do X vigiadas. A primeira e a principal. Tipos:
    #   oficial -> qualquer post citando a artista alerta
    #   artista -> so alerta se citar o Brasil
    #   noticia -> so alerta se citar o Brasil E falar de data/show extra/ingresso esgotado
    "x_accounts": {"livenationbr": "oficial", "ticketmasterbr": "oficial", "espacounimed": "oficial",
                   "HilaryDuff": "artista", "papelpop": "noticia"},
    # so interessa show no Brasil: termos (regex, sem acento; "." no lugar de letra acentuada)
    "brazil_terms": ["brasil", "brazil", "s.o paulo", r"\bsp\b", "espa.o unimed", "rio de janeiro",
                     r"\brj\b", "curitiba", "belo horizonte", "porto alegre", "bras.lia", "recife",
                     "salvador", "fortaleza", "florian.polis"],
    # push no celular: instale o app "ntfy" e assine este topico. "" desliga.
    "ntfy_server": "https://ntfy.sh",
    "ntfy_topic": "",
    "open_browser_on_alert": True,
    "popup_on_alert": True,
    "heartbeat_hours": 24,          # push "monitor vivo" (0 desliga)
    "blind_after_minutes": 5,       # avisa se as paginas ficarem inacessiveis por N minutos
    "news_weak_after_hours": 72,    # manchete so com "data extra" passa a alertar depois disso
}

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/129.0 Safari/537.36")
MESES = {"janeiro": 1, "fevereiro": 2, "marco": 3, "abril": 4, "maio": 5, "junho": 6,
         "julho": 7, "agosto": 8, "setembro": 9, "outubro": 10, "novembro": 11, "dezembro": 12}
HIGH, MED, LOW = "ALTA", "MEDIA", "BAIXA"


# --------------------------------------------------------------------------- util
def load_config():
    cfg = json.loads(json.dumps(DEFAULTS))
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, encoding="utf-8") as f:
            user = json.load(f)
        for k, v in user.items():
            if isinstance(v, dict) and isinstance(cfg.get(k), dict):
                cfg[k].update(v)
            else:
                cfg[k] = v
    return cfg


def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, encoding="utf-8") as f:
            return json.load(f)
    return {}


_last_saved = None


def save_state(state):
    """Grava so quando algo mudou (o loop roda a cada poucos segundos)."""
    global _last_saved
    text = json.dumps(state, ensure_ascii=False, indent=1)
    if text == _last_saved:
        return
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
    os.replace(tmp, STATE_FILE)
    _last_saved = text


def now_str():
    return datetime.now().strftime("%d/%m/%Y %H:%M:%S")


def log(msg):
    line = "[%s] %s" % (now_str(), msg)
    try:
        if os.path.exists(LOG_FILE) and os.path.getsize(LOG_FILE) > 2_000_000:
            os.replace(LOG_FILE, LOG_FILE + ".1")
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except OSError:
        pass
    if sys.stdout:  # None quando roda com pythonw
        try:
            print(line, flush=True)
        except Exception:
            pass


def norm(s):
    s = unicodedata.normalize("NFKD", s or "")
    return "".join(c for c in s if not unicodedata.combining(c)).lower()


def utc_to_local(iso):
    """'2026-10-02T18:00:00Z' -> '02/10/2026 15:00' no fuso do PC."""
    try:
        d = datetime.strptime(iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        return d.astimezone().strftime("%d/%m/%Y %H:%M")
    except (TypeError, ValueError):
        return None


class FetchError(Exception):
    pass


def fetch(url, timeout=25):
    """Retorna (status, url_final, bytes). Nao levanta para 4xx/5xx."""
    req = urllib.request.Request(url, headers={
        "User-Agent": UA, "Accept-Language": "pt-BR,pt;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Encoding": "gzip"})  # pagina de data: ~38 KB em vez de ~240 KB

    def body_of(r):
        data = r.read()
        return gzip.decompress(data) if r.headers.get("Content-Encoding") == "gzip" else data
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.geturl(), body_of(r)
    except urllib.error.HTTPError as e:
        try:
            body = body_of(e)
        except Exception:
            body = b""
        return e.code, e.geturl() or url, body
    except Exception as e:
        raise FetchError("%s: %s" % (type(e).__name__, e))


def slug_url(slug):
    return TM + "/event/" + slug


# --------------------------------------------------------------------------- alertas
def send_ntfy(cfg, title, msg, url, level):
    if not cfg.get("ntfy_topic"):
        return
    body = {"topic": cfg["ntfy_topic"], "title": title, "message": msg,
            "priority": {HIGH: 5, MED: 3, LOW: 2}[level],
            "tags": {HIGH: ["rotating_light"], MED: ["eyes"], LOW: ["white_check_mark"]}[level]}
    if url:
        # tocar na notificacao abre o link; o botao e o link no texto fazem o mesmo
        body["click"] = url
        body["message"] = msg + "\n" + url
        body["actions"] = [{"action": "view", "label": "Abrir ingressos", "url": url}]
    req = urllib.request.Request(cfg["ntfy_server"], data=json.dumps(body).encode("utf-8"),
                                 headers={"Content-Type": "application/json", "User-Agent": UA})
    try:
        urllib.request.urlopen(req, timeout=15).read()
    except Exception as e:
        log("  (ntfy falhou: %s)" % e)


TOAST_PS = (
    "[Windows.UI.Notifications.ToastNotificationManager, Windows.UI.Notifications, ContentType = WindowsRuntime] > $null;"
    "[Windows.Data.Xml.Dom.XmlDocument, Windows.Data.Xml.Dom.XmlDocument, ContentType = WindowsRuntime] > $null;"
    "$d = New-Object Windows.Data.Xml.Dom.XmlDocument; $d.LoadXml($env:HD_XML);"
    "$n = [Windows.UI.Notifications.ToastNotification]::new($d);"
    "[Windows.UI.Notifications.ToastNotificationManager]::CreateToastNotifier("
    "'{1AC14E77-02E7-4E5D-B744-2EB1AE5198B7}\\WindowsPowerShell\\v1.0\\powershell.exe').Show($n)"
)


def toast_xml(title, msg, url=None, sticky=False):
    """Notificacao do Windows. Com url, clicar nela (ou no botao) abre a pagina de ingressos."""
    def esc(s):
        return html.escape(s or "", quote=True)
    attrs = actions = ""
    if url:
        attrs = ' activationType="protocol" launch="%s"%s' % (
            esc(url), ' scenario="reminder"' if sticky else "")  # reminder = fica na tela ate fechar
        actions = ('<actions><action content="Abrir ingressos" activationType="protocol" arguments="%s"/>'
                   '<action content="Fechar" activationType="system" arguments="dismiss"/></actions>') % esc(url)
    return ('<toast%s><visual><binding template="ToastGeneric"><text>%s</text><text>%s</text>'
            '</binding></visual>%s</toast>') % (attrs, esc(title), esc(msg[:300]), actions)


def toast(title, msg, url=None, sticky=False):
    env = dict(os.environ, HD_XML=toast_xml(title, msg, url, sticky))
    try:
        subprocess.Popen(["powershell", "-NoProfile", "-NonInteractive", "-Command", TOAST_PS], env=env,
                         creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception as e:
        log("  (toast falhou: %s)" % e)


def popup_process(title, msg):
    """Roda num processo separado: apita e mostra uma janela que fica por cima de tudo."""
    def beeps():
        try:
            import winsound
            for _ in range(6):
                for f in (880, 1175, 1568):
                    winsound.Beep(f, 180)
                time.sleep(0.4)
        except Exception:
            pass
    threading.Thread(target=beeps, daemon=True).start()
    # MB_ICONWARNING | MB_SYSTEMMODAL | MB_SETFOREGROUND | MB_TOPMOST
    ctypes.windll.user32.MessageBoxW(0, msg, title, 0x30 | 0x1000 | 0x10000 | 0x40000)


def alert(cfg, level, title, msg, url=None, quiet=False):
    log("ALERTA [%s] %s | %s | %s" % (level, title, msg.replace("\n", " / "), url or ""))
    try:
        with open(ALERT_FILE, "a", encoding="utf-8") as f:
            f.write("[%s] [%s] %s\n%s\n%s\n\n" % (now_str(), level, title, msg, url or ""))
    except OSError:
        pass
    send_ntfy(cfg, title, msg, url, level)
    if quiet or os.name != "nt":
        return
    if level in (HIGH, MED):
        toast(title, msg, url, sticky=(level == HIGH))
    if level == HIGH:
        if cfg.get("popup_on_alert"):
            exe = sys.executable
            pyw = os.path.join(os.path.dirname(exe), "pythonw.exe")
            try:
                subprocess.Popen([pyw if os.path.exists(pyw) else exe, os.path.abspath(__file__),
                                  "--popup", title, msg + ("\n\nLink: " + url if url else "")],
                                 creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
            except Exception as e:
                log("  (popup falhou: %s)" % e)
        if cfg.get("open_browser_on_alert") and url:
            try:
                webbrowser.open(url)
            except Exception:
                pass


# --------------------------------------------------------------------------- parsers
def parse_boot(raw):
    """Dados do evento embutidos na pagina (App.bootstrapData). None se a pagina nao e um evento."""
    k = raw.find(b"App.bootstrapData(")
    if k < 0:
        return None
    chunk = raw[raw.find(b"{", k):]
    text = None
    for enc in ("utf-8", "cp1252", "latin-1"):
        try:
            text = chunk.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    try:
        obj, _ = json.JSONDecoder().raw_decode(text)
    except ValueError:
        return None
    return ((obj.get("model") or {}).get("data")) or None


def clean(s):
    s = html.unescape(re.sub(r"<[^>]+>", " ", s or "")).replace("�", "")
    return re.sub(r"\s+", " ", s).strip()


def key(s):
    """Chave de comparacao que ignora acentos/caracteres quebrados (o site as vezes os entrega errados)."""
    return " ".join(w for w in re.split(r"[\s|]+", (s or "").lower()) if re.fullmatch(r"[a-z0-9/:.\-]+", w))


def parse_buttons(h, data=None):
    """Botoes de data na pagina principal: {href: {title, label, cta}}.
    Le do HTML e tambem dos widgets embutidos no JSON da pagina (o que vier)."""
    out = {}
    for chunk in re.split(r"""class=["']button_item["']""", h)[1:]:
        m = re.search(r"""<a\s+href=["']([^"']+)["']""", chunk)
        if not m:
            continue

        def grab(pat):
            g = re.search(pat, chunk, re.S)
            return clean(g.group(1)) if g else ""
        out[m.group(1)] = {
            "title": grab(r"""button_item__title["']>(.*?)</div>"""),
            "label": grab(r"""class=["']cms-label["'][^>]*>(.*?)</span>"""),
            "cta": grab(r"""button_item__cta["']>(.*?)</span>"""),
        }
    for w in (data or {}).get("widgetComponents") or []:
        for b in ((w.get("state") or {}).get("buttons") or []):
            link = b.get("link")
            if not link or link in out:
                continue
            desc = (((b.get("description") or {}).get("md") or {}).get("result")) or ""
            g = re.search(r"""cms-label["'][^>]*>(.*?)</span>""", desc, re.S)
            out[link] = {"title": clean(b.get("title")), "label": clean(g.group(1)) if g else "",
                         "cta": clean(b.get("buttonText"))}
    return out


def visible_text(h):
    """Texto visivel, com uma quebra de linha a cada bloco (paragrafo, div, item...)."""
    h = re.sub(r"(?is)<(script|style)\b.*?</\1>", " ", h)
    h = re.sub(r"(?i)</?(p|div|br|li|ul|ol|h[1-6]|tr|td|table|section|article)\b[^>]*>", "\n", h)
    return html.unescape(re.sub(r"(?s)<[^>]+>", " ", h))


def in_brazil(text, cfg):
    return re.search("|".join(cfg["brazil_terms"]), norm(text)) is not None


def brazil_dates(h, cfg):
    """Datas citadas so em frases que falam do Brasil (a pagina tambem lista a turne em outros paises)."""
    out = set()
    for seg in re.split(r"\n|(?<=[.!?])\s+", visible_text(h)):
        if in_brazil(seg, cfg):
            out.update(date_tokens(seg))
    return out


def date_tokens(text):
    """Todas as datas citadas, normalizadas para 'DD/MM'."""
    t = norm(text)
    out = set()
    for d, m in re.findall(r"\b(\d{1,2})\s*[ºo°�]?\s+de\s+(%s)\b" % "|".join(MESES), t):
        out.add("%02d/%02d" % (int(d), MESES[m]))
    for d, m in re.findall(r"\b(\d{1,2})/(\d{1,2})(?:/\d{2,4})?\b", t):
        if 1 <= int(d) <= 31 and 1 <= int(m) <= 12:
            out.add("%02d/%02d" % (int(d), int(m)))
    return sorted(out)


def summarize_event(data, cfg):
    """Resumo compacto de uma pagina de data, para comparar entre rodadas."""
    qc = data.get("queueConfiguration") or {}
    venue = data.get("venue") or {}
    s = {
        "venue": clean((venue.get("name") or "").split(" - ")[0]), "address": clean(venue.get("description")),
        "name": data.get("name"), "salesStatus": data.get("salesStatus"),
        "notStarted": data.get("notStarted"), "description": data.get("description"),
        "salesStartTime": qc.get("salesStartTime") or data.get("salesStart"),
        "queue": data.get("queueEventName"), "shows": {},
    }
    sold_out = data.get("salesStatus") == "SOLD_OUT"
    for show in data.get("shows") or []:
        sectors = {}
        for sec in show.get("sectors") or []:
            rates = [r for r in (sec.get("rates") or []) if r.get("available")]
            boxes = sorted(x.get("name") for x in (sec.get("sections") or []) if x.get("available"))
            prices = [r.get("price") for r in (sec.get("rates") or []) if r.get("price")]
            sectors[sec.get("name")] = {
                "available": (bool(rates) or bool(boxes)) and not sold_out,
                "sections": [] if sold_out else boxes,
                "price": max(prices) if prices else None,
            }
        s["shows"][str(show.get("id"))] = {
            "date": show.get("startDateFormat") or show.get("name"), "sectors": sectors}
    return s


def watched(name, cfg):
    return any(w in norm(name) for w in cfg["watch_sectors"])


def describe_event(s, cfg):
    lines = [s.get("name") or "?", "Status: %s%s" % (
        s.get("salesStatus"), " (venda ainda nao abriu)" if s.get("notStarted") else "")]
    if s.get("venue"):
        lines.append("Local: %s%s" % (s["venue"], " - " + s["address"] if s.get("address") else ""))
    start = utc_to_local(s.get("salesStartTime"))
    if start:
        lines.append("Abertura das vendas: %s" % start)
    found = False
    for show in s.get("shows", {}).values():
        for name, sec in show["sectors"].items():
            if watched(name, cfg):
                found = True
                lines.append("%s (%s): %s%s%s" % (
                    name, show["date"],
                    "DISPONIVEL" if sec["available"] else "indisponivel",
                    " [%s]" % ", ".join(sec["sections"]) if sec.get("sections") else "",
                    " - R$ %s" % sec["price"] if sec.get("price") else ""))
    if not found and s.get("salesStatus") == "SOLD_OUT":
        lines.append("Camarote: esgotado (data inteira esgotada) - sigo vigiando caso volte")
    return "\n".join(lines)


# --------------------------------------------------------------------------- checks
def fetch_event(slug, cfg):
    status, final, raw = fetch(slug_url(slug))
    if status in (403, 429):
        raise FetchError("HTTP %s (bloqueio temporario?)" % status)
    if status != 200:
        raise FetchError("HTTP %s" % status)
    data = parse_boot(raw)
    return summarize_event(data, cfg) if data else None


def register_event(state, cfg, slug, source, baseline):
    """Registra uma pagina de data recem-descoberta. Alerta se nao for a rodada inicial."""
    events = state.setdefault("events", {})
    if slug in events:
        return
    try:
        summary = fetch_event(slug, cfg)
    except FetchError as e:
        summary = None
        log("  nao consegui abrir %s: %s" % (slug, e))
    if summary is None and source == "palpite":
        return
    events[slug] = summary or {"name": slug, "shows": {}}
    if baseline:
        log("  base: %s -> %s" % (slug, (summary or {}).get("salesStatus")))
        return
    alert(cfg, HIGH, "NOVA DATA HILARY DUFF?! (%s)" % source,
          "Apareceu uma pagina nova: /event/%s\n%s" % (
              slug, describe_event(summary, cfg) if summary else "(sem detalhes ainda)"),
          slug_url(slug))


def slug_from_href(href):
    m = re.search(r"/event/([^/?#\"']+)", href)
    return m.group(1) if m else None


def check_landing(state, cfg, baseline):
    status, final, raw = fetch(cfg["landing_url"])
    in_queue = "queue-it.net" in final
    if in_queue != state.get("landing_in_queue", False):
        state["landing_in_queue"] = in_queue
        if in_queue and not baseline:
            alert(cfg, MED, "Hilary Duff: pagina entrou em fila virtual",
                  "A pagina do evento esta redirecionando para a fila - costuma indicar venda abrindo.",
                  cfg["landing_url"])
    if in_queue:
        return
    if status in (403, 429):
        state["penalty_until"] = time.time() + 900
    if status != 200:
        raise FetchError("HTTP %s" % status)
    h = raw.decode("utf-8", "replace")
    data = parse_boot(raw)
    buttons = parse_buttons(h, data)
    if not buttons and not data:
        raise FetchError("pagina veio sem conteudo de evento")

    i = h.find('class="event_blocks"')
    i = i if i >= 0 else max(h.find("<main"), 0)
    j = h.find("</main>", i)
    region = h[i:j if j > 0 else len(h)]
    # so datas do Brasil: frases que citam o Brasil + os proprios botoes de data
    dates = brazil_dates(region, cfg)
    for b in buttons.values():
        dates.update(date_tokens(b["title"] + " " + b["label"]))
    dates = sorted(dates)
    # qualquer outro link de pagina de data da artista dentro da pagina (alem dos botoes)
    main_slug = slug_from_href(cfg["landing_url"])
    for s in set(re.findall(r"ticketmaster\.com\.br/event/([A-Za-z0-9_\-]+)", region)):
        if s != main_slug and re.search(cfg["artist_regex"], s, re.I) and s not in state.get("events", {}):
            register_event(state, cfg, s, "link na pagina principal", baseline or "landing" not in state)
    cur = {"buttons": buttons, "dates": dates,
           "description": (data or {}).get("description"),
           "salesStatus": (data or {}).get("salesStatus"),
           "showsRange": json.dumps((data or {}).get("showsRange"), sort_keys=True)}
    old = state.get("landing")
    if old is None:
        state["landing"] = cur
        log("  base pagina principal: %d botoes, datas citadas %s" % (len(buttons), dates))
        for href in buttons:
            s = slug_from_href(href)
            if s:
                register_event(state, cfg, s, "pagina principal", True)
        return

    # sumiu tudo de uma vez = resposta estranha, nao atualiza a base
    if old["buttons"] and not buttons:
        raise FetchError("botoes sumiram (resposta incompleta?)")

    for href, b in buttons.items():
        if href not in old["buttons"]:
            s = slug_from_href(href)
            if s and "ticketmaster.com.br" in href and s not in state.get("events", {}):
                register_event(state, cfg, s, "novo botao: %s" % b["title"], False)
            else:
                alert(cfg, HIGH, "NOVO BOTAO na pagina da Hilary Duff",
                      "%s - %s\n%s" % (b["title"], b["label"], href), href)
        else:
            ob = old["buttons"][href]
            if [key(ob[k]) for k in ("label", "cta", "title")] != [key(b[k]) for k in ("label", "cta", "title")]:
                reopened = "esgot" in norm(ob["label"] + ob["cta"]) and "esgot" not in norm(b["label"] + b["cta"])
                alert(cfg, HIGH if reopened else MED,
                      "Hilary Duff: %s mudou de status" % (b["title"] or "data"),
                      "Antes: %s / %s\nAgora: %s / %s" % (ob["label"], ob["cta"], b["label"], b["cta"]), href)
    new_dates = [d for d in dates if d not in old["dates"]]
    if new_dates:
        august = [d for d in new_dates if d.endswith("/08")]
        alert(cfg, HIGH if august else MED, "Hilary Duff: NOVA DATA citada na pagina",
              "Datas novas no texto da pagina: %s" % ", ".join(new_dates), cfg["landing_url"])
    for fld, label in (("description", "descricao"), ("salesStatus", "status"), ("showsRange", "periodo")):
        if cur[fld] != old.get(fld):
            alert(cfg, MED, "Hilary Duff: %s do evento mudou" % label,
                  "Antes: %s\nAgora: %s" % (old.get(fld), cur[fld]), cfg["landing_url"])
    cur["dates"] = sorted(set(dates) | set(old["dates"]))  # nunca "desaprende" uma data
    state["landing"] = cur


POOL = ThreadPoolExecutor(max_workers=6)


def check_events(state, cfg, baseline):
    known = []
    for slug in list(dict.fromkeys(cfg["event_slugs"] + list(state.get("events", {})))):
        if slug not in state.get("events", {}):
            register_event(state, cfg, slug, "lista inicial", True)
        else:
            known.append(slug)

    def get(slug):
        try:
            return fetch_event(slug, cfg)
        except FetchError as e:
            return e
    results = dict(zip(known, POOL.map(get, known)))  # todas as datas ao mesmo tempo
    errors = [r for r in results.values() if isinstance(r, FetchError)]
    if known and len(errors) == len(known):
        if any("HTTP 403" in str(e) or "HTTP 429" in str(e) for e in errors):
            state["penalty_until"] = time.time() + 900  # site reclamou: desacelera por 15 min
        raise FetchError(str(errors[0]))
    for slug in known:
        old = state["events"][slug]
        cur = results[slug]
        if cur is None or isinstance(cur, FetchError):
            continue  # pagina momentaneamente sem dados; mantem a base
        url = slug_url(slug)
        if cur["salesStatus"] != old.get("salesStatus"):
            alert(cfg, HIGH if cur["salesStatus"] == "IN_SALE" else MED,
                  "Hilary Duff: status de venda mudou",
                  "%s\nAntes: %s\nAgora: %s" % (cur["name"], old.get("salesStatus"), cur["salesStatus"]), url)
        if cur["description"] != old.get("description") and old.get("description"):
            alert(cfg, HIGH, "Hilary Duff: datas do evento mudaram",
                  "Antes: %s\nAgora: %s" % (old.get("description"), cur["description"]), url)
        if set(cur["shows"]) - set(old.get("shows", {})) and old.get("shows"):
            alert(cfg, HIGH, "Hilary Duff: nova sessao na pagina", describe_event(cur, cfg), url)
        for sid, show in cur["shows"].items():
            oshow = old.get("shows", {}).get(sid, {"sectors": {}})
            for name, sec in show["sectors"].items():
                osec = oshow["sectors"].get(name, {"available": False, "sections": []})
                freed = [x for x in sec["sections"] if x not in osec.get("sections", [])]
                cd = state.setdefault("cooldown", {})
                ck = "%s|%s" % (slug, name)
                opened = (sec["available"] and not osec["available"]) or bool(freed)
                # no maximo 1 alerta a cada 5 min por setor, caso fique piscando
                if watched(name, cfg) and opened and time.time() - cd.get(ck, 0) > 300:
                    cd[ck] = time.time()
                    alert(cfg, HIGH, "CAMAROTE DISPONIVEL - Hilary Duff %s" % show["date"],
                          "%s liberou ingresso no show de %s!%s%s\nToque/clique para abrir a pagina de compra." % (
                              name, show["date"], " - R$ %s" % sec["price"] if sec.get("price") else "",
                              "\nCamarotes livres: " + ", ".join(sec["sections"]) if sec["sections"] else ""), url)
                elif sec["available"] != osec["available"]:
                    log("  %s | %s: %s" % (show["date"], name,
                                           "voltou a ter ingresso" if sec["available"] else "esgotou"))
        state["events"][slug] = cur


RR = {}  # posicao do rodizio de palpites


def take(name, items, cfg):
    """Rodada completa (--once / primeira vez) devolve tudo; no loop devolve um item por vez, em rodizio."""
    if cfg.get("_sweep") or not items:
        return items
    RR[name] = (RR.get(name, -1) + 1) % len(items)
    return [items[RR[name]]]


def check_slugs(state, cfg, baseline):
    todo = [s for s in cfg["candidate_slugs"] if s not in state.get("events", {})]
    for slug in take("slugs", todo, cfg):
        register_event(state, cfg, slug, "palpite", baseline)


def check_home(state, cfg, baseline):
    status, final, raw = fetch(TM + "/")
    if status != 200:
        raise FetchError("HTTP %s" % status)
    h = raw.decode("utf-8", "replace")
    links = sorted({s for s in re.findall(r"""event/([A-Za-z0-9_\-]+)""", h)
                    if re.search(cfg["artist_regex"], s, re.I)})
    if not links and state.get("home_links"):
        raise FetchError("home veio sem os links conhecidos")
    main_slug = slug_from_href(cfg["landing_url"])
    for s in links:
        if s != main_slug and s not in state.get("events", {}):
            register_event(state, cfg, s, "home da Ticketmaster", baseline or "home_links" not in state)
    state["home_links"] = links


def check_queue(state, cfg, baseline):
    found = state.setdefault("queue_found", [])
    for q in take("queue", [q for q in cfg["queue_candidates"] if q not in found], cfg):
        try:
            status, final, _ = fetch(cfg["queue_base"] + q)
        except FetchError as e:
            log("  fila %s: %s" % (q, e))
            continue
        if status == 200 and "/error" not in final:
            found.append(q)
            if not baseline:
                alert(cfg, HIGH, "FILA NOVA criada: %s" % q,
                      "A Ticketmaster criou uma fila virtual chamada '%s'.\n"
                      "Isso costuma acontecer ANTES da venda abrir - fique de olho na pagina." % q,
                      cfg["landing_url"])


def check_livenation(state, cfg, baseline):
    status, final, raw = fetch(cfg["livenation_url"])
    if status != 200:
        raise FetchError("HTTP %s" % status)
    h = raw.decode("utf-8", "replace")
    # a pagina lista a turne inteira; fica so com os eventos cujo local e no Brasil
    pairs = re.findall(r'"country":"([^"]+)","url":"[^"]*vdp\d+".{0,800}?"url":"(/event/[^"]+?edp\d+)"',
                       h.replace('\\"', '"'), re.S)
    links = sorted({u for c, u in pairs if c in ("Brazil", "Brasil")})
    if not pairs:  # formato mudou: cai no modo antigo (links de Sao Paulo)
        links = sorted(set(re.findall(r"/event/hilary-duff[a-z0-9\-]*sao-paulo[a-z0-9\-]*edp\d+", h)))
    old = state.get("ln_links")
    if old is None:
        log("  base Live Nation: %d eventos no Brasil" % len(links))
    elif not links and old:
        raise FetchError("Live Nation veio sem os eventos conhecidos")
    else:
        for l in links:
            if l not in old:
                alert(cfg, HIGH, "Live Nation listou NOVO show da Hilary Duff no Brasil",
                      "Novo evento na pagina da artista:\n" + l, "https://www.livenation.com.br" + l)
        links = sorted(set(links) | set(old))
    state["ln_links"] = links


STRONG = [r"terceir[ao] (data|show|apresenta)", r"\b3[ªa] data", r"nov[ao]s? (data|show)",
          r"mais um[a]? (data|show|apresenta)", r"segunda data extra", r"mais uma noite",
          r"\b(12|15)/08\b", r"\b(12|15) de agosto", r"dias? (12|15)\b"]
WEAK =[r"data extra", r"show extra", r"esgota"]


def check_news(state, cfg, baseline):
    seen = state.setdefault("news_seen", [])
    first = "news_since" not in state
    if first:
        state["news_since"] = time.time()
    weak_ok = time.time() - state["news_since"] > cfg["news_weak_after_hours"] * 3600
    for feed in cfg["news_feeds"]:
        try:
            status, final, raw = fetch(feed)
            if status != 200:
                raise FetchError("HTTP %s" % status)
            root = ET.fromstring(raw)
        except (FetchError, ET.ParseError) as e:
            log("  noticias: %s" % e)
            continue
        for item in root.iter("item"):
            title = (item.findtext("title") or "").strip()
            link = (item.findtext("link") or "").strip()
            guid = (item.findtext("guid") or link or title).strip()
            if guid in seen:
                continue
            seen.append(guid)
            if first or baseline:
                continue
            t = norm(title)
            if "hilary" not in t:
                continue
            try:
                age = datetime.now(timezone.utc) - parsedate_to_datetime(item.findtext("pubDate"))
                if age > timedelta(days=2):
                    continue  # materia antiga que so entrou no feed agora
            except Exception:
                pass
            if not in_brazil(title, cfg):
                log("  noticia nova ignorada (nao cita o Brasil): %s" % title)
                continue
            strong = any(re.search(p, t) for p in STRONG)
            weak = any(re.search(p, t) for p in WEAK)
            if strong:
                alert(cfg, HIGH, "NOTICIA: possivel nova data da Hilary Duff", title, link)
            elif weak and weak_ok:
                alert(cfg, MED, "Noticia sobre Hilary Duff (data extra?)", title, link)
            else:
                log("  noticia nova (sem alerta): %s" % title)
    del seen[:-800]


X_API = "https://api.fxtwitter.com/2/profile/%s/statuses"
EXTRA = (r"data extra|show extra|nova data|novo show"
         r"|one more (night|show|date)|(new|extra|third|3rd|another|added) (date|show|night)|(date|show|night) added")


def x_rotation(cfg):
    """A primeira conta da lista (a produtora) entra a cada duas checagens; as outras se revezam."""
    accts = list(cfg["x_accounts"])
    rot = []
    for a in accts[1:]:
        rot += [accts[0], a]
    return rot or accts


def check_x(state, cfg, baseline):
    """Posts novos de contas do X. O X nao deixa ler sem login, entao usa a API publica do FxTwitter
    (servico de terceiros: le a timeline de contas especificas, nao faz busca por palavra)."""
    seen_all = state.setdefault("x_seen", {})
    accounts = list(cfg["x_accounts"]) if cfg.get("_sweep") else take("x", x_rotation(cfg), {})
    errors = []
    for acct in accounts:
        results = None
        for attempt in range(3 if cfg.get("_sweep") else 2):  # a API falha ~1 em cada 4 pedidos
            try:
                status, final, raw = fetch(X_API % acct, timeout=12)
                if status != 200:
                    raise FetchError("HTTP %s" % status)
                results = json.loads(raw.decode("utf-8", "replace")).get("results") or []
                break
            except (FetchError, ValueError) as e:
                err = "@%s: %s" % (acct, e)
                time.sleep(0.7)
        if results is None:
            errors.append(err)
            continue
        first = acct not in seen_all
        if first and not results:
            # a API as vezes devolve lista vazia; so aceita "conta sem posts" como base apos 5 vezes
            empty = state.setdefault("x_empty", {})
            empty[acct] = empty.get(acct, 0) + 1
            if empty[acct] < 5:
                continue
        seen = seen_all.setdefault(acct, [])
        for t in results:
            tid = str(t.get("id"))
            if tid in seen:
                continue
            seen.append(tid)
            if first or baseline:
                continue
            if time.time() - (t.get("created_timestamp") or 0) > 48 * 3600:
                continue  # post antigo que so apareceu agora na lista (fixado, repost...)
            text = t.get("text") or ""
            n = norm(text)
            if not re.search(cfg["artist_regex"] + "|lucky me", n):
                continue
            kind = cfg["x_accounts"][acct]
            extra = any(re.search(p, n) for p in STRONG) or re.search(EXTRA, n)
            if kind != "oficial" and not in_brazil(text, cfg):
                log("  X @%s: post ignorado (nao cita o Brasil): %s" % (acct, text[:80].replace("\n", " ")))
                continue
            if kind == "noticia" and not (extra or any(re.search(p, n) for p in WEAK)):
                continue
            # se so cita datas que ja existem (13/08, 14/08), e lembrete/divulgacao: alerta mais brando
            known = set((state.get("landing") or {}).get("dates", []))
            cited = set(date_tokens(text))
            is_new = bool(extra) and not (cited and cited <= known)
            who = (t.get("author") or {}).get("screen_name") or acct
            alert(cfg, HIGH if is_new else MED,
                  "X: @%s postou sobre Hilary Duff%s" % (who, " - NOVA DATA?" if is_new else ""),
                  "%s\n\nPost: %s" % (text[:400], t.get("url")),
                  cfg["landing_url"] if is_new else t.get("url"))
        del seen[:-300]
    if errors and len(errors) == len(accounts):
        raise FetchError("; ".join(errors))


CHECKS = [("landing", check_landing), ("events", check_events), ("slugs", check_slugs),
          ("home", check_home), ("queue", check_queue), ("livenation", check_livenation),
          ("news", check_news), ("x", check_x)]


# --------------------------------------------------------------------------- loop
def run_checks(state, cfg, names):
    baseline = not state.get("baseline_done")
    for name, fn in CHECKS:
        if name not in names:
            continue
        fails = state.setdefault("fail", {})
        since = state.setdefault("fail_since", {})
        blind = state.setdefault("blind", {})
        what = {"landing": "a pagina do evento", "events": "as paginas das datas (camarote)"}
        try:
            fn(state, cfg, baseline)
            if blind.pop(name, None):
                alert(cfg, LOW, "Monitor Hilary Duff voltou ao normal",
                      "Voltei a conseguir abrir %s." % what[name], cfg["landing_url"], quiet=True)
            fails[name] = 0
            since.pop(name, None)
        except FetchError as e:
            fails[name] = fails.get(name, 0) + 1
            t0 = since.setdefault(name, time.time())
            if fails[name] <= 3 or fails[name] % 50 == 0:  # nao enche o log quando roda a cada 5 s
                log("  %s falhou (%dx): %s" % (name, fails[name], e))
            if name in what and time.time() - t0 > cfg["blind_after_minutes"] * 60 and not blind.get(name):
                blind[name] = True
                alert(cfg, MED, "Monitor Hilary Duff esta CEGO",
                      "Nao consigo abrir %s ha %d min (%s). Confira manualmente." % (
                          what[name], (time.time() - t0) // 60, e), cfg["landing_url"])
        except Exception as e:  # erro inesperado num check nao derruba os outros
            log("  %s: erro inesperado %s: %s" % (name, type(e).__name__, e))
    if baseline and "landing" in state:
        state["baseline_done"] = True
        state["baseline_at"] = now_str()
        log("Base registrada. A partir de agora, qualquer mudanca gera alerta.")
    save_state(state)


def status_text(state, cfg):
    out = ["Base registrada em: %s" % state.get("baseline_at", "(ainda nao)")]
    for href, b in (state.get("landing") or {}).get("buttons", {}).items():
        out.append("  botao: %s [%s] -> %s" % (b["title"], b["label"], href))
    out.append("  datas citadas na pagina: %s" % ", ".join((state.get("landing") or {}).get("dates", [])))
    for slug, ev in state.get("events", {}).items():
        out.append("- /event/%s" % slug)
        out.extend("    " + l for l in describe_event(ev, cfg).split("\n"))
    out.append("Live Nation (Brasil): %d eventos" % len(state.get("ln_links") or []))
    out.append("Filas encontradas: %s" % (state.get("queue_found") or "nenhuma nova"))
    out.append("Noticias ja vistas: %d" % len(state.get("news_seen") or []))
    out.append("X: " + (", ".join("@%s (%d posts vistos)" % (a, len(v))
                                  for a, v in (state.get("x_seen") or {}).items()) or "ainda sem base"))
    return "\n".join(out)


def single_instance():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("127.0.0.1", 47613))
        s.listen(1)
        return s
    except OSError:
        return None


def loop(cfg):
    lock = single_instance()
    if not lock:
        log("Ja existe um monitor rodando. Saindo.")
        return
    with open(PID_FILE, "w") as f:
        f.write(str(os.getpid()))
    state = load_state()
    log("Monitor iniciado (pid %d). Push ntfy: %s" % (os.getpid(), cfg.get("ntfy_topic") or "desligado"))
    if not state.get("baseline_done"):  # primeira vez: rodada completa para registrar a base
        run_checks(state, dict(cfg, _sweep=True), [n for n, _ in CHECKS])
    last = {}
    stats = {"t": time.time(), "n": 0, "secs": 0.0, "others": []}
    try:
        while True:
            now = time.time()
            slow = 5 if now < state.get("penalty_until", 0) else 1

            def due(n):
                return now - last.get(n, 0) >= cfg["intervals"][n] * slow * random.uniform(0.9, 1.1)
            # as datas (camarote) rodam sempre que vencem; das outras checagens, so UMA por volta,
            # para nenhuma checagem lenta segurar a do camarote
            todo = ["events"] if due("events") else []
            others = [n for n, _ in CHECKS if n != "events" and due(n)]
            if others:
                todo.append(min(others, key=lambda n: last.get(n, 0)))
            if todo:
                for n in todo:
                    last[n] = now
                run_checks(state, cfg, todo)
                if "events" in todo:
                    stats["n"] += 1
                    stats["secs"] += time.time() - now
                stats["others"] += [n for n in todo if n != "events"]
            if time.time() - stats["t"] >= 60:  # um resumo por minuto no log
                ev = state.get("events", {})
                log("ok: datas checadas %dx no ultimo minuto (%.1fs cada)%s | %s%s" % (
                    stats["n"], stats["secs"] / max(stats["n"], 1),
                    " + " + ",".join(sorted(set(stats["others"]))) if stats["others"] else "",
                    " ".join("%s=%s" % (s.replace("vg-hilary-duff", "vg") or "vg", e.get("salesStatus"))
                             for s, e in ev.items()),
                    " | MODO LENTO (site pediu para desacelerar)" if slow > 1 else ""))
                stats = {"t": time.time(), "n": 0, "secs": 0.0, "others": []}
            hb = cfg.get("heartbeat_hours") or 0
            if hb and now - state.get("last_heartbeat", 0) > hb * 3600:
                if state.get("last_heartbeat"):
                    alert(cfg, LOW, "Monitor Hilary Duff: vivo",
                          "Sigo vigiando. %d datas conhecidas, nada novo." % len(state.get("events", {})),
                          cfg["landing_url"], quiet=True)
                state["last_heartbeat"] = now
                save_state(state)
            time.sleep(1)
    except KeyboardInterrupt:
        log("Monitor encerrado pelo usuario.")
    finally:
        try:
            os.remove(PID_FILE)
        except OSError:
            pass


def stop():
    if not os.path.exists(PID_FILE):
        print("Nenhum monitor em segundo plano encontrado.")
        return
    pid = open(PID_FILE).read().strip()
    subprocess.call(["taskkill", "/PID", pid, "/F"])
    try:
        os.remove(PID_FILE)
    except OSError:
        pass


def main():
    if sys.stdout:
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass
    ap = argparse.ArgumentParser(description="Monitor de nova data - Hilary Duff / Ticketmaster BR")
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--status", action="store_true")
    ap.add_argument("--test-alert", action="store_true")
    ap.add_argument("--stop", action="store_true")
    ap.add_argument("--popup", nargs=2, metavar=("TITULO", "MSG"), help=argparse.SUPPRESS)
    a = ap.parse_args()
    if a.popup:
        return popup_process(*a.popup)
    cfg = load_config()
    if a.stop:
        return stop()
    if a.status:
        return print(status_text(load_state(), cfg))
    if a.test_alert:
        return alert(cfg, HIGH, "TESTE - Monitor Hilary Duff",
                     "Se voce esta vendo isto, o alerta funciona.\nE assim que vou avisar de uma nova data.",
                     cfg["landing_url"])
    if a.once:
        state = load_state()
        run_checks(state, dict(cfg, _sweep=True), [n for n, _ in CHECKS])
        return print(status_text(state, cfg))
    loop(cfg)


if __name__ == "__main__":
    main()
