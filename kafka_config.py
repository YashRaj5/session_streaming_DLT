# Databricks notebook source
import os
bootstrapServers = dbutils.secrets.get("confluent-kafka", 
"boostrap_server")
confluentApiKey = dbutils.secrets.get("confluent-kafka", "api_key")
confluentApiSecret = dbutils.secrets.get("confluent-kafka", "api_secret")
