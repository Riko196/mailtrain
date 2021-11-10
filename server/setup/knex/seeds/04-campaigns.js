"use strict";
const { getRandomCampaign } = require("../data-generator");

exports.seed = (knex, Promise) =>
  (async () => {
    // Generates new random campaigns
    const countOfCampaigns = 5;
    for (let i = 1; i <= countOfCampaigns; i++) {
      const randomCampaign = getRandomCampaign();
      const campaign = await knex("campaigns").select("*").where("id", i).first();
      if (campaign) {
        await knex("campaigns").update(randomCampaign).where("id", campaign.id);
      } else {
        await knex("campaigns").insert(randomCampaign);
      }
    }
  })();
