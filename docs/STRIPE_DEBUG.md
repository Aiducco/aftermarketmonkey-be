# Stripe Debug – Subscription Not Showing

## ⚠️ Most common cause: Live vs Test key mismatch

Your subscriptions exist in **Stripe Live** mode (you used `sk_live_...` in curl).  
If `STRIPE_SECRET_KEY` in your backend is set to `sk_test_...`, Stripe returns **empty data** (no subscriptions).

**Fix:** Set `STRIPE_SECRET_KEY` to your **Live** key (`sk_live_...`) so it matches where your subscriptions exist.

Check logs for `stripe_mode=live` or `stripe_mode=test` – it must match your subscription’s mode.

---

## 1. List all subscriptions for your customer

Replace `YOUR_STRIPE_SECRET_KEY` and `cus_UAbK6eyYgGz9oI` (your customer ID from DB):

```bash
curl "https://api.stripe.com/v1/subscriptions?customer=cus_UAbK6eyYgGz9oI&status=all&limit=10" \
  -u YOUR_STRIPE_SECRET_KEY:
```

This returns all subscriptions for that customer. Check:
- `data` array – empty means no subscriptions
- Each subscription has `status` (active, trialing, past_due, canceled, etc.)

## 2. Check your billing API response

Replace with your JWT and API base URL:

```bash
curl -X GET "https://your-api.com/api/billing/subscription/" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -H "Content-Type: application/json"
```

Expected with subscription: `{"subscription": {"plan_id": "...", "plan": "...", ...}}`  
Expected without: `{"subscription": null}`

## 3. Verify company ↔ customer mapping

Ensure the company row in your DB has `stripe_customer_id = cus_UAbK6eyYgGz9oI` and that your JWT’s `company_id` matches that company.

## 4. Check server logs

After the change, logs will show something like:

```
Subscription fetch: customer=cus_xxx company_id=N total=X displayable=Y statuses=[...]
```

- `total=0` → no subscriptions for this customer in Stripe
- `total>0` but `displayable=0` → subscriptions exist but none are active/trialing/past_due
- `displayable>0` → subscription found; if the UI still shows nothing, the issue is likely in the frontend
