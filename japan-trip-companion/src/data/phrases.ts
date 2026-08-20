/**
 * Offline phrase card.
 *
 * Ordered by how often you will actually need each group, not by grammar.
 * The food section comes first because it is the one that matters most on this
 * trip and the one that is hardest to mime.
 *
 * Showing the Japanese on screen works far better than trying to say it —
 * hand the phone over rather than reading aloud.
 */

export interface Phrase {
  /** Japanese as it should be shown to the reader. */
  jp: string
  /** Romanisation, for having a go yourself. */
  romaji: string
  /** What it means. */
  en: string
  /** When to use it, where that is not obvious. */
  note?: string
  /** True for the handful worth knowing before you land. */
  key?: boolean
}

export interface PhraseGroup {
  id: string
  title: string
  blurb: string
  phrases: Phrase[]
}

export const phraseGroups: PhraseGroup[] = [
  {
    id: 'food-rules',
    title: 'Our two rules',
    blurb:
      'The first one does the heavy lifting. Show it rather than say it, and show it before you order rather than after.',
    phrases: [
      {
        jp: '豚肉と魚介類は食べられません。',
        romaji: 'Butaniku to gyokairui wa taberaremasen.',
        en: 'We cannot eat pork or seafood.',
        note: 'The single most useful sentence of the trip. Show this at every restaurant.',
        key: true,
      },
      {
        jp: '豚肉は入っていますか？',
        romaji: 'Butaniku wa haitte imasu ka?',
        en: 'Does this contain pork?',
        key: true,
      },
      {
        jp: '魚介類は入っていますか？',
        romaji: 'Gyokairui wa haitte imasu ka?',
        en: 'Does this contain seafood?',
        key: true,
      },
      {
        jp: 'だしは魚ですか？',
        romaji: 'Dashi wa sakana desu ka?',
        en: 'Is the broth made with fish?',
        note: 'The question behind every "order carefully" note in this app. Udon, soba, okonomiyaki batter, miso soup.',
        key: true,
      },
      {
        jp: 'かつおぶしを抜いてください。',
        romaji: 'Katsuobushi o nuite kudasai.',
        en: 'Please leave out the bonito flakes.',
        note: 'For okonomiyaki and anything served with shavings on top.',
      },
      {
        jp: '牛肉か鶏肉の料理はありますか？',
        romaji: 'Gyuniku ka toriniku no ryori wa arimasu ka?',
        en: 'Do you have any beef or chicken dishes?',
        note: 'Better than asking what you cannot have — this gets you a recommendation.',
        key: true,
      },
      {
        jp: 'ベジタリアンのメニューはありますか？',
        romaji: 'Bejitarian no menyu wa arimasu ka?',
        en: 'Do you have a vegetarian menu?',
      },
      {
        jp: '食べられないものがあります。',
        romaji: 'Taberarenai mono ga arimasu.',
        en: 'There are some things we cannot eat.',
        note: 'A softer opener before the specifics, if that feels easier.',
      },
    ],
  },
  {
    id: 'restaurant',
    title: 'In a restaurant',
    blurb: 'Enough to get seated, order, and pay without a phrasebook moment.',
    phrases: [
      { jp: '二人です。', romaji: 'Futari desu.', en: 'Two people.', key: true },
      { jp: '予約しています。', romaji: 'Yoyaku shite imasu.', en: 'We have a reservation.' },
      { jp: '予約できますか？', romaji: 'Yoyaku dekimasu ka?', en: 'Can we make a reservation?' },
      {
        jp: '英語のメニューはありますか？',
        romaji: 'Eigo no menyu wa arimasu ka?',
        en: 'Is there an English menu?',
        key: true,
      },
      { jp: 'これをください。', romaji: 'Kore o kudasai.', en: 'This one, please.', note: 'Point at the menu.' },
      { jp: 'おすすめは何ですか？', romaji: 'Osusume wa nan desu ka?', en: 'What do you recommend?' },
      { jp: 'お会計をお願いします。', romaji: 'Okaikei o onegai shimasu.', en: 'The bill, please.' },
      { jp: 'カードで払えますか？', romaji: 'Kado de haraemasu ka?', en: 'Can I pay by card?', note: 'Worth asking in the mountains — several places are cash only.' },
    ],
  },
  {
    id: 'transport',
    title: 'Stations and buses',
    blurb:
      'Most useful on 15 to 17 September, where three of the buses are unreserved and the connections are tight.',
    phrases: [
      {
        jp: '荷物を預けられますか？',
        romaji: 'Nimotsu o azukeraremasu ka?',
        en: 'Can I leave my luggage here?',
        note: 'The Kamikochi baggage room and the Shirakawa-go storage counter.',
        key: true,
      },
      {
        jp: 'コインロッカーはどこですか？',
        romaji: 'Koin rokka wa doko desu ka?',
        en: 'Where are the coin lockers?',
        key: true,
      },
      {
        jp: '次のバスは何時ですか？',
        romaji: 'Tsugi no basu wa nanji desu ka?',
        en: 'What time is the next bus?',
        note: 'For Kamikochi to Hirayu, Hirayu to Takayama, Takayama to Shirakawa-go.',
        key: true,
      },
      {
        jp: '京都行きのホームはどこですか？',
        romaji: 'Kyoto-yuki no homu wa doko desu ka?',
        en: 'Which platform for the train to Kyoto?',
        note: 'Swap in any destination before -yuki.',
      },
      {
        jp: 'この電車は敦賀に止まりますか？',
        romaji: 'Kono densha wa Tsuruga ni tomarimasu ka?',
        en: 'Does this train stop at Tsuruga?',
      },
      { jp: '指定席です。', romaji: 'Shiteiseki desu.', en: 'We have reserved seats.' },
      { jp: '切符を二枚ください。', romaji: 'Kippu o nimai kudasai.', en: 'Two tickets, please.' },
      { jp: '乗り換えはどこですか？', romaji: 'Norikae wa doko desu ka?', en: 'Where do I change trains?' },
    ],
  },
  {
    id: 'hotel',
    title: 'Hotels and the ryokan',
    blurb:
      'The luggage-forwarding phrase on 14 September is the one that matters. The front desk will do the rest.',
    phrases: [
      {
        jp: '宅急便で荷物を送りたいです。',
        romaji: 'Takkyubin de nimotsu o okuritai desu.',
        en: "I'd like to send luggage by takkyubin.",
        note: 'For 14 Sep at Bespoke Hotel Shinjuku, forwarding to the Kyoto hotel.',
        key: true,
      },
      {
        jp: '荷物を預かってもらえますか？',
        romaji: 'Nimotsu o azukatte moraemasu ka?',
        en: 'Could you hold our luggage?',
        note: 'Before check-in and after check-out — every hotel on this trip will.',
        key: true,
      },
      { jp: 'チェックインをお願いします。', romaji: 'Chekkuin o onegai shimasu.', en: 'Checking in, please.' },
      {
        jp: '夕食は何時ですか？',
        romaji: 'Yushoku wa nanji desu ka?',
        en: 'What time is dinner?',
        note: 'Ask at Miyama Ouan on arrival — the ryokan dinner runs at a set sitting.',
        key: true,
      },
      { jp: '朝食は何時からですか？', romaji: 'Choshoku wa nanji kara desu ka?', en: 'When does breakfast start?' },
      { jp: '温泉はいつ入れますか？', romaji: 'Onsen wa itsu hairemasu ka?', en: 'When can we use the onsen?' },
      {
        jp: 'レストランを予約してもらえますか？',
        romaji: 'Resutoran o yoyaku shite moraemasu ka?',
        en: 'Could you book a restaurant for us?',
        note: 'How to get the Hida beef table in Takayama without phoning yourself.',
      },
    ],
  },
  {
    id: 'shops',
    title: 'Shopping',
    blurb: 'Tax-free is the one worth knowing. Bring passports.',
    phrases: [
      {
        jp: '免税はできますか？',
        romaji: 'Menzei wa dekimasu ka?',
        en: 'Can I claim tax-free?',
        note: 'Usually needs a passport and a minimum spend, often around ¥5,000.',
        key: true,
      },
      { jp: '試着できますか？', romaji: 'Shichaku dekimasu ka?', en: 'Can I try this on?' },
      { jp: '他のサイズはありますか？', romaji: 'Hoka no saizu wa arimasu ka?', en: 'Do you have another size?' },
      { jp: 'いくらですか？', romaji: 'Ikura desu ka?', en: 'How much is it?' },
      { jp: '袋をください。', romaji: 'Fukuro o kudasai.', en: 'A bag, please.' },
      { jp: '見ているだけです。', romaji: 'Mite iru dake desu.', en: "I'm just looking." },
    ],
  },
  {
    id: 'basics',
    title: 'Basics and trouble',
    blurb: 'The first two get you a long way on politeness alone.',
    phrases: [
      { jp: 'すみません。', romaji: 'Sumimasen.', en: 'Excuse me / sorry.', note: 'Also how you get a waiter\'s attention.', key: true },
      { jp: 'ありがとうございます。', romaji: 'Arigato gozaimasu.', en: 'Thank you.', key: true },
      { jp: 'お願いします。', romaji: 'Onegai shimasu.', en: 'Please.' },
      { jp: '大丈夫です。', romaji: 'Daijobu desu.', en: "It's fine / no thank you.", note: 'Extremely versatile. Declines almost anything politely.' },
      {
        jp: '英語が話せる人はいますか？',
        romaji: 'Eigo ga hanaseru hito wa imasu ka?',
        en: 'Is there someone who speaks English?',
        key: true,
      },
      { jp: 'わかりません。', romaji: 'Wakarimasen.', en: "I don't understand." },
      { jp: 'トイレはどこですか？', romaji: 'Toire wa doko desu ka?', en: 'Where is the toilet?', key: true },
      { jp: '道に迷いました。', romaji: 'Michi ni mayoimashita.', en: "We're lost." },
      { jp: '助けてください。', romaji: 'Tasukete kudasai.', en: 'Please help.' },
      { jp: '病院はどこですか？', romaji: 'Byoin wa doko desu ka?', en: 'Where is the hospital?' },
    ],
  },
]

/** The short list worth reading on the plane. */
export const keyPhrases = phraseGroups.flatMap((g) => g.phrases.filter((p) => p.key))
