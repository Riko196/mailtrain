'use strict';

const knex = require('../lib/knex');
const { getMongoDB } = require('../lib/mongodb');
const campaigns = require('./campaigns');
const lists = require('./lists');
const subscriptions = require('./subscriptions');
const contextHelpers = require('../lib/context-helpers');
const geoip = require('geoip-ultralight');
const uaParser = require('device');
const he = require('he');
const { getHaProxyUrl } = require('../lib/urls');
const tools = require('../lib/tools');
const shortid = require('../lib/shortid');
const {enforce} = require('../lib/helpers');

const LinkId = {
    OPEN: -1,
    GENERAL_CLICK: 0
};

const LinkStatus = {
    UNSYNCHRONIZED: 0,
    SYNCHRONIZED: 1
};

/* Called only from HAPUBLIC server for getting link from linkCid. */
async function resolve(linkCid) {
    return await getMongoDB().collection('links').findOne({ cid: linkCid });
}

/* Called only from HAPUBLIC server when some subcriber click on some link or open mail. */
async function insertClickedLink(ip, header, campaign, list, subscription, linkId) {
    /* In one DB query, insert a new opened or clicked campaign_link */
    try {
        await getMongoDB().collection('campaign_links').insertOne({
            _id: `${campaign}:${list}:${subscription}:${linkId}`, ip, header, campaign, list, subscription, linkId
        });
    } catch (error) {
        /* We can ignore problem with duplicates */
        if (error.code !== 11000) {
            throw error;
        }
    }
}

/*
 * Called only from Synchronizer with database query for synchronizing data to MySQL when some subscriber opened the mail
 * or clicked on some link.
 */
async function countLink(remoteIp, userAgent, campaignCid, listCid, subscriptionCid, linkId) {
    await knex.transaction(async tx => {
        const list = await lists.getByCidTx(tx, contextHelpers.getAdminContext(), listCid);
        const campaign = await campaigns.getTrackingSettingsByCidTx(tx, campaignCid);
        const subscription = await subscriptions.getByCidTx(tx, contextHelpers.getAdminContext(), list.id, subscriptionCid);

        const country = geoip.lookupCountry(remoteIp) || null;
        const device = uaParser(userAgent, {
            unknownUserAgentDeviceType: 'desktop',
            emptyUserAgentDeviceType: 'desktop'
        });
        const now = new Date();

        const _countLink = async (clickLinkId, incrementOnDup) => {
            try {
                const campaignLinksQry = knex('campaign_links')
                    .insert({
                        campaign: campaign.id,
                        list: list.id,
                        subscription: subscription.id,
                        link: clickLinkId,
                        ip: remoteIp,
                        device_type: device.type,
                        country
                    }).toSQL();

                const campaignLinksQryResult = await tx.raw(campaignLinksQry.sql + (incrementOnDup ? ' ON DUPLICATE KEY UPDATE `count`=`count`+1' : ''), campaignLinksQry.bindings);

                if (campaignLinksQryResult[0].affectedRows > 1) { // When using DUPLICATE KEY UPDATE, this means that the entry was already there
                    return false;
                }

                return true;

            } catch (err) {
                if (err.code === 'ER_DUP_ENTRY') {
                    return false;
                }

                throw err;
            }
        };


        // Update opened and click timestamps
        const latestUpdates = {};

        if (!campaign.click_tracking_disabled && linkId > LinkId.GENERAL_CLICK) {
            latestUpdates.latest_click = now;
        }

        if (!campaign.open_tracking_disabled) {
            latestUpdates.latest_open = now;
        }

        if (latestUpdates.latest_click || latestUpdates.latest_open) {
            await tx(subscriptions.getSubscriptionTableName(list.id)).update(latestUpdates).where('id', subscription.id);
        }

        // Update clicks
        if (linkId > LinkId.GENERAL_CLICK && !campaign.click_tracking_disabled) {
            await tx('links').increment('hits').where('id', linkId);
            if (await _countLink(linkId, true)) {
                await tx('links').increment('visits').where('id', linkId);

                if (await _countLink(LinkId.GENERAL_CLICK, false)) {
                    await tx('campaigns').increment('clicks').where('id', campaign.id);
                }
            }
        }


        // Update opens. We count a click as an open too.
        if (!campaign.open_tracking_disabled) {
            if (await _countLink(LinkId.OPEN, true)) {
                await tx('campaigns').increment('opened').where('id', campaign.id);
            }
        }
    });
}

/* Called only from Synchronizer with database query for inserting existing link from some campaign mail. */
async function insertLinkIfNotExists(link) {
    const foundLink = await knex('links').select(['id', 'cid']).where({
        campaign: link.campaign,
        url: link.url
    }).first();

    if (!foundLink) {
        try {
            const ids = await knex('links').insert(link);
        } catch (error) {
            if (error.code === 'ER_DUP_ENTRY') {
                const foundLink = await knex('links').select(['id', 'cid']).where({
                    campaign: link.campaign,
                    url: link.url
                }).first();

                enforce(foundLink);
            }
        }
    }
}

/* Called only from SenderWorker without database query for adding link to the list or getting link back. */
function addOrGetLink(campaignId, url, links) {
    const link = links.find(link => link.campaign === campaignId && link.url === url);

    if (!link) {
        let cid = shortid.generate();

        links.push({
            campaign: campaignId,
            cid,
            url,
            status: LinkStatus.UNSYNCHRONIZED
        });

        return {
            id: links.length - 1,
            cid
        };
    } else {
        return link;
    }
}

/* Called only from SenderWorker without database query for update links during making mails. */
function updateLinks(source, tagLanguage, mergeTags, campaign, campaignListsById, list, subscription, links) {
    if ((campaign.open_tracking_disabled && campaign.click_tracking_disabled) || !source || !source.trim()) {
        // tracking is disabled, do not modify the message
        return source;
    }

    // insert tracking image
    if (!campaign.open_tracking_disabled) {
        let inserted = false;
        const imgUrl = getHaProxyUrl(`/links/${campaign.cid}/${list.cid}/${subscription.cid}`);
        const img = '<img src="' + imgUrl + '" width="1" height="1" alt="mt">';
        source = source.replace(/<\/body\b/i, match => {
            inserted = true;
            return img + match;
        });
        if (!inserted) {
            source = source + img;
        }
    }

    if (!campaign.click_tracking_disabled) {
        const re = /(<a[^>]* href\s*=\s*["']\s*)(http[^"'>\s]+)/gi;

        const urlsToBeReplaced = new Set();

        source.replace(re, (match, prefix, encodedUrl) => {
            const url = he.decode(encodedUrl, {isAttributeValue: true});
            urlsToBeReplaced.add(url);
        });

        const urls = new Map(); // url -> {id, cid} (as returned by add)
        for (const url of urlsToBeReplaced) {
            // url might include variables, need to rewrite those just as we do with message content
            const expanedUrl = encodeURI(tools.formatCampaignTemplate(url, tagLanguage, mergeTags, false, campaign, campaignListsById, list, subscription));
            const link = addOrGetLink(campaign.id, expanedUrl, links);
            urls.set(url, link);
        }

        source = source.replace(re, (match, prefix, encodedUrl) => {
            const url = he.decode(encodedUrl, {isAttributeValue: true});
            const link = urls.get(url);
            return prefix + (link ? getHaProxyUrl(`/links/${campaign.cid}/${list.cid}/${subscription.cid}/${link.cid}`) : url);
        });
    }

    return source;
}

module.exports.LinkId = LinkId;
module.exports.LinkStatus = LinkStatus;
module.exports.resolve = resolve;
module.exports.countLink = countLink;
module.exports.insertClickedLink = insertClickedLink;
module.exports.insertLinkIfNotExists = insertLinkIfNotExists;
module.exports.updateLinks = updateLinks;
