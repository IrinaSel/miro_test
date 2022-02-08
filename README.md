Welcome to Miro test project made by Iryna S.!

## Solution
Result query is stored in attribution model. This is an incremental model which will be builded using all data from the source models, but on subsequent runs it will use only the latest data.


## Comments:

I assumed that staging tables are stored in marketing schema. 
First I’ve looked into what mediums are in the conversion table.

```select distinct medium from marketing.sessions```

From what I saw it was not clear what Paid Impression is. I assumed that if is_paid is true and medium is 'DISPLAY' then this is Paid Impression. All other mediums with the true is_paid flag are Paid Clicks. If is_paid is false, then this is an organic channel.

To understand whether registration falls into session lifespan I wanted to calculate when this life span ends. I used time_started field and added respective hours depending on whether this session is Paid Impression, Paid Click or Organic. This changes are made in staging sessions model.

Then I decided to define session priority in staging sessions model as well.
From the conditions we know that Paid sessions are the most valuable ones and that is why their priority would be 1, Then we have organic sessions with priority 2 and the last ones are direct sessions with priority 3. Direct sessions have priority 3 since we want to consider them only as a last resort and in case direct session was the first one and organic session was the second one during 12 hours before registration we would like to attribute organic session for this sign up. Defining priorities would help to identify which session to choose if their lifespans would interfire.

In attribution mart I removed all sessions that have a lifespan that doesn’t include registration timestamp with the following condition in the join: 

```c.registration_time between t.time_started and t.time_ended```

To define which session happened first and to choose paid session over organic or direct in case their lifespan interferes I used this function: 

```row_number() over(partition by user_id order by session_priority,time_started) as rn```

Then we can choose all records with rn=1 and replace empty medium with ‘Other’ for registration that didn’t have live sessions.
