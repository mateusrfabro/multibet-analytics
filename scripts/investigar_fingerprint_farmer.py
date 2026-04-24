"""Investigar fingerprint dos farmers — IPs reais, geo, device, navigator."""
import os, sys, json
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.supernova_bet import get_supernova_bet_connection

tunnel, conn = get_supernova_bet_connection()
try:
    with conn.cursor() as cur:
        # ====================================================================
        # 1) Listar TODAS as tabelas que podem ter info de fingerprint
        # ====================================================================
        print("="*90)
        print("1) TABELAS POTENCIAIS PARA FINGERPRINT")
        print("="*90)
        cur.execute("""
            SELECT t.table_name, c.column_name, c.data_type
            FROM information_schema.tables t
            JOIN information_schema.columns c ON c.table_name=t.table_name
            WHERE t.table_schema='public'
              AND (c.column_name ILIKE '%%ip%%' OR c.column_name ILIKE '%%device%%' OR c.column_name ILIKE '%%agent%%'
                   OR c.column_name ILIKE '%%session%%' OR c.column_name ILIKE '%%coord%%' OR c.column_name ILIKE '%%geo%%'
                   OR c.column_name ILIKE '%%fingerprint%%' OR c.column_name ILIKE '%%location%%' OR c.column_name ILIKE '%%metadata%%'
                   OR c.column_name ILIKE '%%audit%%' OR c.column_name ILIKE '%%log%%' OR c.column_name ILIKE '%%track%%')
            ORDER BY t.table_name, c.column_name
        """)
        for r in cur.fetchall():
            print(f"   {r[0]:30} {r[1]:25} {r[2]}")

        # ====================================================================
        # 2) AMOSTRAR user_sessions das contas do batch (ontem + hoje)
        # ====================================================================
        print("\n" + "="*90)
        print("2) USER_SESSIONS — JSONBs das contas do batch (ontem 1980-1997 + hoje 1970-79+1988)")
        print("="*90)
        cur.execute("""
            SELECT u.username, u.phone,
                   s.coords, s.metadata, s.utm,
                   s.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS criada_brt
            FROM user_sessions s JOIN users u ON u.id=s.user_id
            WHERE u.phone LIKE '+92341374%'
            ORDER BY u.phone, s.created_at
            LIMIT 30
        """)
        rows = cur.fetchall()
        print(f"   Total sessions encontradas: {len(rows)}")
        for r in rows[:10]:
            print(f"\n   --- {r[0]} ({r[1]}) sess criada {str(r[5])[:19]} ---")
            print(f"   coords:   {json.dumps(r[2]) if r[2] else 'null'}")
            print(f"   metadata: {json.dumps(r[3])[:200] if r[3] else 'null'}")
            print(f"   utm:      {json.dumps(r[4])[:120] if r[4] else 'null'}")

        # ====================================================================
        # 3) Listar campos JSON distintos em metadata pra entender estrutura
        # ====================================================================
        print("\n" + "="*90)
        print("3) ESTRUTURA do JSONB metadata (user_sessions)")
        print("="*90)
        cur.execute("""
            SELECT DISTINCT jsonb_object_keys(metadata) AS chave
            FROM user_sessions
            WHERE metadata IS NOT NULL
            ORDER BY 1
        """)
        keys_meta = [r[0] for r in cur.fetchall()]
        print(f"   Chaves no metadata: {keys_meta}")

        cur.execute("""
            SELECT DISTINCT jsonb_object_keys(coords) AS chave
            FROM user_sessions
            WHERE coords IS NOT NULL
            ORDER BY 1
        """)
        keys_coords = [r[0] for r in cur.fetchall()]
        print(f"   Chaves no coords: {keys_coords}")

        cur.execute("""
            SELECT DISTINCT jsonb_object_keys(utm) AS chave
            FROM user_sessions
            WHERE utm IS NOT NULL
            ORDER BY 1
        """)
        keys_utm = [r[0] for r in cur.fetchall()]
        print(f"   Chaves no utm: {keys_utm}")

        # ====================================================================
        # 4) Se houver IP no metadata, agrupar por IP entre contas do batch
        # ====================================================================
        print("\n" + "="*90)
        print("4) IPs/Device dos batches — concentracao entre contas suspeitas")
        print("="*90)
        # Tentar várias chaves possíveis
        for ip_key in ['ip', 'ip_address', 'remote_ip', 'client_ip', 'real_ip', 'x-forwarded-for']:
            cur.execute(f"""
                SELECT s.metadata->>'{ip_key}' AS ip,
                       COUNT(DISTINCT u.id) AS users_distintos,
                       array_agg(DISTINCT u.username) AS usernames
                FROM user_sessions s JOIN users u ON u.id=s.user_id
                WHERE u.phone LIKE '+92341374%'
                  AND s.metadata->>'{ip_key}' IS NOT NULL
                GROUP BY 1
                ORDER BY users_distintos DESC
                LIMIT 5
            """)
            rows = cur.fetchall()
            if rows:
                print(f"\n   [chave='{ip_key}']")
                for r in rows:
                    flag = " <- SUSPEITO" if r[1] >= 2 else ""
                    print(f"     {str(r[0])[:30]:30} users={r[1]}{flag}")
                    if r[1] <= 5:
                        print(f"        usernames={r[2]}")

        # User-agent
        for ua_key in ['user_agent', 'userAgent', 'ua', 'browser']:
            cur.execute(f"""
                SELECT s.metadata->>'{ua_key}' AS ua,
                       COUNT(DISTINCT u.id) AS users
                FROM user_sessions s JOIN users u ON u.id=s.user_id
                WHERE u.phone LIKE '+92341374%'
                  AND s.metadata->>'{ua_key}' IS NOT NULL
                GROUP BY 1
                ORDER BY users DESC
                LIMIT 5
            """)
            rows = cur.fetchall()
            if rows:
                print(f"\n   [chave='{ua_key}']")
                for r in rows:
                    print(f"     users={r[1]}  {str(r[0])[:80]}")

        # Coords - lat/lng
        print(f"\n   COORDS - geolocalizacao:")
        cur.execute("""
            SELECT s.coords->>'latitude' AS lat,
                   s.coords->>'longitude' AS lng,
                   COUNT(DISTINCT u.id) AS users
            FROM user_sessions s JOIN users u ON u.id=s.user_id
            WHERE u.phone LIKE '+92341374%' AND s.coords IS NOT NULL
            GROUP BY 1,2
            ORDER BY users DESC
            LIMIT 10
        """)
        for r in cur.fetchall():
            flag = " <- SUSPEITO" if r[2] >= 3 else ""
            print(f"     lat={r[0]:>12} lng={r[1]:>12} users={r[2]}{flag}")

        # ====================================================================
        # 5) Verificar webhook_deliveries (tem request_body com IP do cliente)
        # ====================================================================
        print("\n" + "="*90)
        print("5) WEBHOOK_DELIVERIES — pode ter IP real do gateway de pagamento")
        print("="*90)
        cur.execute("""
            SELECT column_name, data_type FROM information_schema.columns
            WHERE table_schema='public' AND table_name='webhook_deliveries'
            ORDER BY ordinal_position
        """)
        for r in cur.fetchall():
            print(f"   {r[0]:30} {r[1]}")

        # Sample
        cur.execute("""
            SELECT *
            FROM webhook_deliveries
            ORDER BY created_at DESC
            LIMIT 1
        """)
        row = cur.fetchone()
        if row:
            cols = [d[0] for d in cur.description]
            print("\n   Sample webhook delivery (recente):")
            for c, v in zip(cols, row):
                if v and len(str(v)) < 200:
                    print(f"     {c}: {v}")
                elif v:
                    print(f"     {c}: {str(v)[:150]}...")

        # ====================================================================
        # 6) Cruzamento — todas as contas do batch tem mesma chave?
        # ====================================================================
        print("\n" + "="*90)
        print("6) FINGERPRINT CONSOLIDADO - todas contas batch (ontem+hoje)")
        print("="*90)
        cur.execute("""
            SELECT u.username, u.phone,
                   COUNT(DISTINCT s.id) AS n_sessions,
                   array_agg(DISTINCT s.coords->>'latitude') FILTER (WHERE s.coords IS NOT NULL) AS lats,
                   array_agg(DISTINCT s.coords->>'longitude') FILTER (WHERE s.coords IS NOT NULL) AS lngs,
                   array_agg(DISTINCT s.metadata->>'platform') FILTER (WHERE s.metadata IS NOT NULL) AS platforms,
                   array_agg(DISTINCT s.metadata->>'language') FILTER (WHERE s.metadata IS NOT NULL) AS languages,
                   array_agg(DISTINCT s.metadata->>'timezone') FILTER (WHERE s.metadata IS NOT NULL) AS timezones,
                   array_agg(DISTINCT s.metadata->>'screen') FILTER (WHERE s.metadata IS NOT NULL) AS screens
            FROM users u
            LEFT JOIN user_sessions s ON s.user_id=u.id
            WHERE u.phone LIKE '+92341374%'
            GROUP BY u.username, u.phone
            ORDER BY u.phone
        """)
        for r in cur.fetchall():
            print(f"   {str(r[0])[:18]:18} {r[1]:14} sess={r[2]}")
            if r[3]: print(f"      lat={r[3]} lng={r[4]}")
            if r[5]: print(f"      platf={r[5]}")
            if r[6]: print(f"      lang={r[6]}")
            if r[7]: print(f"      tz={r[7]}")
            if r[8]: print(f"      screen={r[8]}")

finally:
    conn.close(); tunnel.stop()
