/**
 * Packing list, built around the PDF's luggage split rather than as a generic
 * list. The whole point is which bag a thing goes in:
 *
 *   Big case  -> forwarded Tokyo to Kyoto on 14 Sep. You do not see it 15-18 Sep.
 *   Trolley   -> the only bag you have for the Alps and Kanazawa, 15-18 Sep.
 *   Day bag   -> on you, every day.
 *   Documents -> on you, and backed up offline.
 *
 * These are recommendations, not from the PDF.
 */

export interface PackingItem {
  id: string
  label: string
  /** Why it is in this bag rather than another, when that is the interesting part. */
  why?: string
}

export interface PackingGroup {
  id: string
  title: string
  /** Which bag, and when it matters. */
  bag: string
  blurb: string
  items: PackingItem[]
}

export const packingGroups: PackingGroup[] = [
  {
    id: 'trolley',
    title: 'Small trolley',
    bag: '15 – 18 Sep · the only bag you have',
    blurb:
      'Four days across Kamikochi, Hirayu, Takayama, Shirakawa-go and Kanazawa. Pack this one properly on the evening of 14 Sep — once the big cases leave for Kyoto, whatever is missing stays missing.',
    items: [
      { id: 'pk-t-shoes', label: 'Proper walking shoes, worn not packed', why: '5–6 km of riverside boardwalk and gravel at Kamikochi.' },
      { id: 'pk-t-rain', label: 'Light rain shell each', why: 'A jacket, not an umbrella — the Kamikochi path gets slippery and the wind makes umbrellas useless.' },
      { id: 'pk-t-warm', label: 'A warm mid layer each', why: 'Kamikochi is 1,500 m up. Mornings can be 8–10 °C while Tokyo is 27 °C.' },
      { id: 'pk-t-days', label: '4 days of clothes', why: 'Two onsen nights in the middle, so you need less than you think.' },
      { id: 'pk-t-onsen', label: 'Something to sleep in', why: 'Both ryokan provide yukata, but bring your own if you would rather.' },
      { id: 'pk-t-cash', label: 'Cash, including coins', why: 'Myojin Pond ¥500 each at the gate, the Kamikochi baggage room, the Shirakawa-go lockers, the unreserved bus tickets. No useful ATMs in the valley.' },
      { id: 'pk-t-charger', label: 'Chargers and a power bank', why: 'Long day on 15 Sep with the app, maps and camera all running.' },
      { id: 'pk-t-meds', label: 'All medication', why: 'Never goes in a bag you cannot reach for four days.' },
      { id: 'pk-t-water', label: 'A water bottle each', why: 'Nothing to buy between Kappa Bridge and Myojin except one food stop.' },
      { id: 'pk-t-sun', label: 'Sun hat and sunscreen', why: 'Nara Park and the Kamikochi riverbank are both shadeless.' },
      { id: 'pk-t-daypack', label: 'A small daypack', why: 'For the Kamikochi walk once the trolley is in the baggage room.' },
    ],
  },
  {
    id: 'bigcase',
    title: 'Big cases',
    bag: 'Forwarded 14 Sep · next seen in Kyoto, 18 Sep',
    blurb:
      'Everything for Kyoto and Osaka. It leaves Shinjuku on 14 Sep and should be waiting at Hotel Resol Trinity when you arrive at 19:38 on the 18th — so nothing in here can be needed before then.',
    items: [
      { id: 'pk-b-kyoto', label: 'Clothes for Kyoto and Osaka, 18–25 Sep', why: 'Warm and humid — 28–29 °C. Light, breathable, and more changes than you would pack at home.' },
      { id: 'pk-b-smart', label: 'One smarter outfit each', why: 'For the wagyu dinners — Hafuu, Yoroniku, Matsusakagyu Yakiniku M.' },
      { id: 'pk-b-shoes2', label: 'A second pair of shoes', why: 'Kyoto is long days on stone. Rotating shoes matters more than the shoes themselves.' },
      { id: 'pk-b-toiletries', label: 'Bulk toiletries', why: 'Every hotel supplies the basics — this is the refill.' },
      { id: 'pk-b-space', label: 'Deliberate empty space', why: 'Tokyo shopping happens before this case is forwarded. Osaka shopping has to fit on the way home — you have 30 kg each in the hold, so the room is real.' },
    { id: 'pk-b-scale', label: 'Know what your hand luggage weighs', why: 'Economy allows 7 kg cabin, and that is the limit that actually catches people. Heavy things go in the hold case.' },
      { id: 'pk-b-bag', label: 'A foldable extra bag', why: 'The cheapest insurance against a shopping trip you enjoyed too much.' },
    ],
  },
  {
    id: 'daybag',
    title: 'Day bag',
    bag: 'Every day',
    blurb: 'What comes with you when the luggage is at the hotel.',
    items: [
      { id: 'pk-d-umbrella', label: 'Folding umbrella', why: 'Tokyo showers are short and heavy. Any convenience store sells one for about ¥600 if you skip it.' },
      { id: 'pk-d-battery', label: 'Power bank and cable' },
      { id: 'pk-d-coins', label: 'Coin purse', why: 'Japan gives you a lot of ¥100 coins and you will want them for lockers and vending machines.' },
      { id: 'pk-d-bag', label: 'A tote for shopping', why: 'Shops charge for bags, and you will be carrying things by mid-afternoon.' },
      { id: 'pk-d-towel', label: 'A small hand towel', why: 'Many public toilets have no dryer. This is why everyone in Japan carries one.' },
      { id: 'pk-d-wipes', label: 'Hand sanitiser or wipes', why: 'Street food in Dotonbori and Nishiki is eaten standing up.' },
    ],
  },
  {
    id: 'documents',
    title: 'Documents and phone',
    bag: 'On you · and saved offline',
    blurb:
      "The PDF's own checklist lives here. Everything on this list should work with the phone in airplane mode.",
    items: [
      { id: 'pk-doc-passport', label: 'Passports', why: 'Also needed for every tax-free purchase.' },
      { id: 'pk-doc-suica', label: 'Suica in Apple Wallet, with a balance loaded', why: "From the PDF. Set it up at home where the wifi is good — there is no JR Pass on this trip." },
      { id: 'pk-doc-tickets', label: 'Offline copies of the four booked tickets', why: 'From the PDF: Azusa 1, National Park Liner, Shirakawa-go to Kanazawa, Kanazawa to Kyoto. Screenshots, not links.' },
    { id: 'pk-doc-boarding', label: 'Emirates boarding passes for EK 312 and EK 317', why: 'Checked in online, saved to Wallet. Seats 33A out and 62K home are already assigned.' },
    { id: 'pk-doc-vjw', label: 'Visit Japan Web completed', why: 'Turns the 22:20 arrival at Haneda into a QR scan instead of a queue.' },
      { id: 'pk-doc-hotels', label: 'Hotel confirmations and addresses in Japanese', why: 'The Japanese address is what a taxi driver needs.' },
      { id: 'pk-doc-app', label: 'This app added to the home screen', why: 'The itinerary is bundled in, so it works with no signal.' },
      { id: 'pk-doc-insurance', label: 'Travel insurance details' },
      { id: 'pk-doc-esim', label: 'eSIM or pocket wifi sorted before you fly', why: 'Cheaper arranged at home, and you want data from the moment you land.' },
      { id: 'pk-doc-cards', label: 'Two payment cards, kept separately', why: 'Some Japanese sites reject foreign cards — teamLab and Shibuya Sky both can.' },
    ],
  },
]
