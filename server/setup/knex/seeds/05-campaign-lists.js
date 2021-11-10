"use strict";
const { getRandomCampaign } = require("../data-generator");

exports.seed = (knex, Promise) =>
  (async () => {
    // Generates new random campaigns
    const countOfLists = (await knex("lists").select("id")).length;
    const countOfCampaigns = (await knex("campaigns").select("id")).length;
    for (let i = 1; i <= countOfCampaigns + countOfLists; i++) {
      const randomCampaignList = {
        campaign: Math.floor(Math.random() * countOfCampaigns) + 1,
        list: Math.floor(Math.random() * countOfLists) + 1
      };
      const campaignList = await knex("campaign_lists").select("*").where("id", i).first();
      if (campaignList) {
        await knex("campaign_lists").update(randomCampaignList).where("id", campaignList.id);
      } else {
        await knex("campaign_lists").insert(randomCampaignList);
      }
    }
  })();
