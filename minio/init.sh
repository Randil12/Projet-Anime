#!/bin/sh
# ============================================================================
# Init MinIO : buckets + RBAC
# Exécuté par le service minio-init au démarrage.
# Les variables d'env (MINIO_USER, MINIO_PASSWORD, MINIO_DE_*, MINIO_DS_*)
# sont injectées par docker-compose.
# ============================================================================
set -e

mc alias set myminio http://minio:9000 "$MINIO_USER" "$MINIO_PASSWORD"

# === Buckets ===
mc mb --ignore-existing myminio/bronze
mc mb --ignore-existing myminio/silver
mc mb --ignore-existing myminio/ml-models

# === Policies RBAC ===
mc admin policy create myminio data-engineer /policies/data-engineer.json || true
mc admin policy create myminio data-scientist /policies/data-scientist.json || true

# === Users + attachement des policies ===
mc admin user add myminio "$MINIO_DE_USER" "$MINIO_DE_PASSWORD" || true
mc admin policy attach myminio data-engineer --user "$MINIO_DE_USER" || true

mc admin user add myminio "$MINIO_DS_USER" "$MINIO_DS_PASSWORD" || true
mc admin policy attach myminio data-scientist --user "$MINIO_DS_USER" || true

echo "✅ Buckets + RBAC MinIO initialisés"
