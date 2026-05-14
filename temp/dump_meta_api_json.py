"""
Dump JSON bruto da Meta Marketing API — TODOS os niveis de objeto.

Mauro quer ver "tudo que a API retorna". Este script chama 1 amostra de
cada nivel pedindo um conjunto AMPLO de fields, e salva em JSON
estruturado pra leitura/copia.

Niveis dumpados (Graph API v21.0):
  1) Account     /act_XXX
  2) Campaign    /{campaign_id}
  3) AdSet       /{adset_id}
  4) Ad          /{ad_id}
  5) Creative    /{creative_id}
  6) Insights    /{campaign_id}/insights (todos os fields uteis)

Conta amostral: act_1418521646228655 (Multibet in-house, sempre tem dados recentes).
"""
import os, sys, json, urllib.request, urllib.parse, urllib.error
from datetime import date, timedelta
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__) + "/.."))
from dotenv import load_dotenv
load_dotenv()

API = "v21.0"
TOKEN = os.getenv("META_ADS_ACCESS_TOKEN")
ACC = "act_1418521646228655"  # Multibet (in-house)

# Conjuntos amplos de fields por endpoint
ACCOUNT_FIELDS = (
    # Campos basicos — compativeis com permissao ads_read (token BM2 atual).
    # Removidos: business_name, business_country_code, end_advertiser_name,
    # funding_source, owner, capabilities, user_role -> exigem business_management.
    "id,account_id,name,account_status,age,amount_spent,balance,created_time,"
    "currency,disable_reason,spend_cap,timezone_name,timezone_offset_hours_utc"
)

CAMPAIGN_FIELDS = (
    "id,name,account_id,objective,status,effective_status,configured_status,"
    "buying_type,special_ad_categories,daily_budget,lifetime_budget,"
    "budget_remaining,bid_strategy,start_time,stop_time,created_time,updated_time,"
    "source_campaign_id,spend_cap,can_use_spend_cap"
)

ADSET_FIELDS = (
    "id,name,campaign_id,account_id,status,effective_status,configured_status,"
    "daily_budget,lifetime_budget,bid_amount,bid_strategy,billing_event,"
    "optimization_goal,destination_type,promoted_object,attribution_setting,"
    "frequency_control_specs,learning_stage_info,start_time,end_time,"
    "created_time,updated_time,targeting"
)

AD_FIELDS = (
    "id,name,adset_id,campaign_id,account_id,status,effective_status,"
    "configured_status,bid_amount,conversion_specs,tracking_specs,"
    "preview_shareable_link,created_time,updated_time,creative"
)

CREATIVE_FIELDS = (
    "id,name,title,body,image_url,video_id,thumbnail_url,url_tags,"
    "object_type,object_story_id,object_story_spec,call_to_action_type,"
    "asset_feed_spec,instagram_actor_id,link_destination_display_url,link_url,"
    "effective_authorization_category,effective_object_story_id,link_og_id,"
    "status,template_url"
)

# Insights — quase tudo que faz sentido pra leitura de performance + atribuicao
INSIGHTS_FIELDS = (
    "spend,impressions,clicks,reach,frequency,ctr,cpc,cpm,cpp,"
    "actions,action_values,conversions,cost_per_action_type,cost_per_conversion,"
    "cost_per_inline_link_click,cost_per_thruplay,cost_per_unique_action_type,"
    "cost_per_unique_click,date_start,date_stop,inline_link_clicks,"
    "inline_post_engagement,instant_experience_clicks_to_open,"
    "instant_experience_clicks_to_start,instant_experience_outbound_clicks,"
    "mobile_app_purchase_roas,objective,optimization_goal,purchase_roas,"
    "social_spend,unique_actions,unique_clicks,unique_ctr,"
    "unique_inline_link_click_ctr,unique_link_clicks_ctr,unique_outbound_clicks,"
    "unique_outbound_clicks_ctr,video_30_sec_watched_actions,"
    "video_avg_time_watched_actions,video_p25_watched_actions,"
    "video_p50_watched_actions,video_p75_watched_actions,"
    "video_p100_watched_actions,video_play_actions,video_thruplay_watched_actions,"
    "video_time_watched_actions,website_ctr,website_purchase_roas"
)


def _get(url):
    try:
        with urllib.request.urlopen(url, timeout=90) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode()[:500]
        return {"_error": {"http_code": e.code, "body": body}}
    except Exception as e:
        return {"_error": str(e)}


def fetch(path: str, fields: str, extra_qs: str = "") -> dict:
    """GET helper — retorna o JSON inteiro do response."""
    base = f"https://graph.facebook.com/{API}/{path}?access_token={TOKEN}&fields={fields}"
    if extra_qs:
        base += "&" + extra_qs
    return _get(base)


out = {
    "api_version": API,
    "fetched_at": date.today().isoformat(),
    "sample_account": ACC,
    "notes": (
        "Dump bruto da Meta Marketing API com fields amplos em cada nivel. "
        "Cada chave abaixo eh a resposta CRUA da Graph API (sem nenhum filtro). "
        "Pipeline atual (sync_meta_spend.py) usa SO o nivel 'insights_campaign' "
        "filtrando alguns fields. Niveis ad/creative trazem as UTMs (url_tags)."
    ),
    "endpoints": {}
}

# ---------------------------------------------------------------- 1) Account
print("1) Account...")
out["endpoints"]["1_account"] = {
    "url": f"https://graph.facebook.com/{API}/{ACC}?fields={ACCOUNT_FIELDS}",
    "response": fetch(ACC, ACCOUNT_FIELDS),
}

# ---------------------------------------------------------------- 2) Campanhas
print("2) Campaigns (top 2 por id)...")
camps = fetch(f"{ACC}/campaigns", CAMPAIGN_FIELDS, "limit=2")
out["endpoints"]["2_campaigns_list"] = {
    "url": f"https://graph.facebook.com/{API}/{ACC}/campaigns?fields={CAMPAIGN_FIELDS}&limit=2",
    "response": camps,
}

# pega 1 campaign_id pra usar nos proximos niveis
sample_camp_id = None
if isinstance(camps, dict) and camps.get("data"):
    sample_camp_id = camps["data"][0].get("id")
    out["sample_campaign_id"] = sample_camp_id

# ---------------------------------------------------------------- 3) AdSets
sample_adset_id = None
if sample_camp_id:
    print(f"3) AdSets de {sample_camp_id}...")
    adsets = fetch(f"{sample_camp_id}/adsets", ADSET_FIELDS, "limit=2")
    out["endpoints"]["3_adsets_of_sample_campaign"] = {
        "url": f"https://graph.facebook.com/{API}/{sample_camp_id}/adsets?fields={ADSET_FIELDS}&limit=2",
        "response": adsets,
    }
    if isinstance(adsets, dict) and adsets.get("data"):
        sample_adset_id = adsets["data"][0].get("id")
        out["sample_adset_id"] = sample_adset_id

# ---------------------------------------------------------------- 4) Ads
sample_ad_id = None
sample_creative_id = None
if sample_adset_id:
    print(f"4) Ads de {sample_adset_id}...")
    ads = fetch(f"{sample_adset_id}/ads", AD_FIELDS, "limit=2")
    out["endpoints"]["4_ads_of_sample_adset"] = {
        "url": f"https://graph.facebook.com/{API}/{sample_adset_id}/ads?fields={AD_FIELDS}&limit=2",
        "response": ads,
    }
    if isinstance(ads, dict) and ads.get("data"):
        sample_ad_id = ads["data"][0].get("id")
        cre = ads["data"][0].get("creative", {})
        if isinstance(cre, dict):
            sample_creative_id = cre.get("id")
        out["sample_ad_id"] = sample_ad_id
        out["sample_creative_id"] = sample_creative_id

# ---------------------------------------------------------------- 5) Creative
if sample_creative_id:
    print(f"5) Creative {sample_creative_id}...")
    out["endpoints"]["5_creative_full"] = {
        "url": f"https://graph.facebook.com/{API}/{sample_creative_id}?fields={CREATIVE_FIELDS}",
        "response": fetch(sample_creative_id, CREATIVE_FIELDS),
    }

# ---------------------------------------------------------------- 6) Insights
if sample_camp_id:
    print(f"6) Insights de {sample_camp_id} (ultimos 7d, diario)...")
    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=6)
    tr = urllib.parse.quote(json.dumps({"since": str(start), "until": str(end)}))
    qs = f"time_range={tr}&time_increment=1&level=campaign&limit=10"
    out["endpoints"]["6_insights_campaign_7d"] = {
        "url": f"https://graph.facebook.com/{API}/{sample_camp_id}/insights?fields={INSIGHTS_FIELDS}&{qs}",
        "response": fetch(f"{sample_camp_id}/insights", INSIGHTS_FIELDS, qs),
    }

# ---------------------------------------------------------------- 7) Insights — breakdowns disponiveis
print("7) Insights com breakdown (ex: device_platform)...")
if sample_camp_id:
    qs = (f"time_range={tr}&time_increment=1&level=campaign&limit=10"
          f"&breakdowns=device_platform,publisher_platform")
    out["endpoints"]["7_insights_with_breakdown_device_publisher"] = {
        "url": f"https://graph.facebook.com/{API}/{sample_camp_id}/insights?{qs}",
        "response": fetch(f"{sample_camp_id}/insights",
                          "spend,impressions,clicks,reach,actions", qs),
    }

# ---------------------------------------------------------------- 8) Lista de TODOS os fields possiveis (metadata)
print("8) Metadata — fields disponiveis em cada endpoint...")
out["endpoints"]["8_metadata_account_fields"] = {
    "note": "Metadata: o Facebook expoe lista de fields disponiveis no proprio objeto",
    "url": f"https://graph.facebook.com/{API}/{ACC}?metadata=1",
    "response": fetch(ACC, "id", "metadata=1"),
}

# ---------------------------------------------------------------- Salva
out_path = os.path.join("reports", f"meta_api_dump_{date.today().strftime('%Y%m%d')}.json")
os.makedirs("reports", exist_ok=True)
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(out, f, indent=2, ensure_ascii=False)

print(f"\nOK. JSON salvo em: {out_path}")
print(f"Tamanho: {os.path.getsize(out_path):,} bytes")
