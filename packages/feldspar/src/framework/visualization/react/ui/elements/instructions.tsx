import { Translator } from "../../../../translator";
import { ReactFactoryContext } from "../../factory";
import { Title3 } from "./text";
import TwitterSvg from "../../../../../assets/images/twitter.svg";
import FacebookSvg from "../../../../../assets/images/facebook.svg";
import InstagramSvg from "../../../../../assets/images/instagram.svg";
import YoutubeSvg from "../../../../../assets/images/youtube.svg";
import TextBundle from "../../../../text_bundle";
import { Bullet } from "./bullet";
import { JSX } from "react";
import React from "react";

const linkTwitter: string = "https://eyra.co";
const linkFacebook: string = "https://eyra.co";
const linkInstagram: string = "https://eyra.co";
const linkYoutube: string = "https://eyra.co";

interface InstructionsProps {
  platform: string;
  locale: string;
}

type Props = InstructionsProps & ReactFactoryContext;

export const Instructions = (props: Props): JSX.Element => {
  const { title } = prepareCopy(props);
  const { locale } = props;
  const platform = props.platform.toLowerCase();

  function renderBullets(bullets: string[]): JSX.Element[] {
    return bullets.map((bullet) => renderBullet(bullet));
  }

  function renderContent(): JSX.Element {
    return (
      <>
        <div className="flex flex-col gap-4 text-bodymedium font-body text-grey2">
          {renderBullets(bullets[platform][locale] ?? bullets[platform].en)}
          {links[platform][locale] ?? links[platform].en}
        </div>
      </>
    );
  }

  return (
    <div className="flex flex-col gap-6 p-8 border-2 border-grey4 rounded">
      <div className="flex flex-row gap-8 items-center">
        <div className="grow">
          <Title3 text={title} margin="" />
        </div>
        <div className="h-12">
          <img className="h-12" src={icon[platform]} />
        </div>
      </div>
      {renderContent()}
    </div>
  );
};

interface Copy {
  title: string;
}

function prepareCopy({ platform, locale }: Props): Copy {
  return {
    title: Translator.translate(title, locale),
  };
}

const title = new TextBundle()
  .add("en", "Download")
  .add("nl", "Download")
  .add("de", "Herunterladen")
  .add("pl", "Pobierz")
  .add("tr", "İndir")
  .add("ar", "تنزيل")
  .add("ru", "Скачать")
  .add("it", "Scarica")
  .add("ro", "Descarcă")
  .add("es", "Descargar")
  .add("sq", "Shkarko");

function renderBullet(text: string): JSX.Element {
  return (
    <Bullet frameSize="w-5 h-30px">
      <div>{text}</div>
    </Bullet>
  );
}

// --- Twitter ---
const bulletsTwitterEn: string[] = [
  "Check the email that you received from Twitter",
  "Click on the download link and store the file",
  "Choose the stored file and continue",
];
const bulletsTwitterNl: string[] = [
  "Ga naar de email die u ontvangen heeft van Twitter.",
  'Klik op de link "gedownload” en sla het bestand op',
  "Kies het bestand en ga verder.",
];
const bulletsTwitterDe: string[] = [
  "Prüfen Sie die E-Mail, die Sie von Twitter erhalten haben",
  "Klicken Sie auf den Download-Link und speichern Sie die Datei",
  "Wählen Sie die gespeicherte Datei aus und fahren Sie fort",
];
const bulletsTwitterPl: string[] = [
  "Sprawdź e-mail, który otrzymałeś/aś od Twittera",
  "Kliknij link do pobrania i zapisz plik",
  "Wybierz zapisany plik i kontynuuj",
];
const bulletsTwitterTr: string[] = [
  "Twitter'dan aldığın e-postayı kontrol et",
  "İndirme bağlantısına tıkla ve dosyayı kaydet",
  "Kaydedilen dosyayı seç ve devam et",
];
const bulletsTwitterAr: string[] = [
  "تحقق من البريد الإلكتروني الذي تلقيته من تويتر",
  "انقر على رابط التنزيل واحفظ الملف",
  "اختر الملف المحفوظ وتابع",
];
const bulletsTwitterRu: string[] = [
  "Проверьте письмо, полученное от Twitter",
  "Нажмите на ссылку для скачивания и сохраните файл",
  "Выберите сохранённый файл и продолжите",
];
const bulletsTwitterIt: string[] = [
  "Controlla l'email ricevuta da Twitter",
  "Clicca sul link di download e salva il file",
  "Scegli il file salvato e continua",
];
const bulletsTwitterRo: string[] = [
  "Verifică e-mailul pe care l-ai primit de la Twitter",
  "Fă clic pe linkul de descărcare și salvează fișierul",
  "Alege fișierul salvat și continuă",
];
const bulletsTwitterEs: string[] = [
  "Consulte el correo electrónico que recibió de Twitter",
  "Haga clic en el enlace de descarga y guarde el archivo",
  "Elija el archivo guardado y continúe",
];
const bulletsTwitterSq: string[] = [
  "Kontrollo email-in që ke marrë nga Twitter",
  "Kliko te lidhja e shkarkimit dhe ruaje skedarin",
  "Zgjidh skedarin e ruajtur dhe vazhdo",
];

// --- Facebook ---
const bulletsFacebookEn: string[] = [
  "Check the email that you received from Facebook",
  "Click on the download link and store the file",
  "Choose the stored file and continue",
];
const bulletsFacebookNl: string[] = [
  "Ga naar de email die u ontvangen heeft van Facebook.",
  "Klik op de link “Je gegevens downloaden” en sla het bestand op.",
  "Kies het bestand en ga verder.",
];
const bulletsFacebookDe: string[] = [
  "Prüfen Sie die E-Mail, die Sie von Facebook erhalten haben",
  "Klicken Sie auf den Download-Link und speichern Sie die Datei",
  "Wählen Sie die gespeicherte Datei aus und fahren Sie fort",
];
const bulletsFacebookPl: string[] = [
  "Sprawdź e-mail, który otrzymałeś/aś od Facebooka",
  "Kliknij link do pobrania i zapisz plik",
  "Wybierz zapisany plik i kontynuuj",
];
const bulletsFacebookTr: string[] = [
  "Facebook'tan aldığın e-postayı kontrol et",
  "İndirme bağlantısına tıkla ve dosyayı kaydet",
  "Kaydedilen dosyayı seç ve devam et",
];
const bulletsFacebookAr: string[] = [
  "تحقق من البريد الإلكتروني الذي تلقيته من فيسبوك",
  "انقر على رابط التنزيل واحفظ الملف",
  "اختر الملف المحفوظ وتابع",
];
const bulletsFacebookRu: string[] = [
  "Проверьте письмо, полученное от Facebook",
  "Нажмите на ссылку для скачивания и сохраните файл",
  "Выберите сохранённый файл и продолжите",
];
const bulletsFacebookIt: string[] = [
  "Controlla l'email ricevuta da Facebook",
  "Clicca sul link di download e salva il file",
  "Scegli il file salvato e continua",
];
const bulletsFacebookRo: string[] = [
  "Verifică e-mailul pe care l-ai primit de la Facebook",
  "Fă clic pe linkul de descărcare și salvează fișierul",
  "Alege fișierul salvat și continuă",
];
const bulletsFacebookEs: string[] = [
  "Consulte el correo electrónico que recibió de Facebook",
  "Haga clic en el enlace de descarga y guarde el archivo",
  "Elija el archivo guardado y continúe",
];
const bulletsFacebookSq: string[] = [
  "Kontrollo email-in që ke marrë nga Facebook",
  "Kliko te lidhja e shkarkimit dhe ruaje skedarin",
  "Zgjidh skedarin e ruajtur dhe vazhdo",
];

// --- Instagram ---
const bulletsInstagramEn: string[] = [
  "Check the email that you received from Instagram",
  "Click on the download link and store the file",
  "Choose the stored file and continue",
];
const bulletsInstagramNl: string[] = [
  "Ga naar de email die u ontvangen heeft van Instagram.",
  "Klik op de link “Gegevens downloaden” en sla het bestand op.",
  "Kies het bestand en ga verder.",
];
const bulletsInstagramDe: string[] = [
  "Prüfen Sie die E-Mail, die Sie von Instagram erhalten haben",
  "Klicken Sie auf den Download-Link und speichern Sie die Datei",
  "Wählen Sie die gespeicherte Datei aus und fahren Sie fort",
];
const bulletsInstagramPl: string[] = [
  "Sprawdź e-mail, który otrzymałeś/aś od Instagrama",
  "Kliknij link do pobrania i zapisz plik",
  "Wybierz zapisany plik i kontynuuj",
];
const bulletsInstagramTr: string[] = [
  "Instagram'dan aldığın e-postayı kontrol et",
  "İndirme bağlantısına tıkla ve dosyayı kaydet",
  "Kaydedilen dosyayı seç ve devam et",
];
const bulletsInstagramAr: string[] = [
  "تحقق من البريد الإلكتروني الذي تلقيته من إنستغرام",
  "انقر على رابط التنزيل واحفظ الملف",
  "اختر الملف المحفوظ وتابع",
];
const bulletsInstagramRu: string[] = [
  "Проверьте письмо, полученное от Instagram",
  "Нажмите на ссылку для скачивания и сохраните файл",
  "Выберите сохранённый файл и продолжите",
];
const bulletsInstagramIt: string[] = [
  "Controlla l'email ricevuta da Instagram",
  "Clicca sul link di download e salva il file",
  "Scegli il file salvato e continua",
];
const bulletsInstagramRo: string[] = [
  "Verifică e-mailul pe care l-ai primit de la Instagram",
  "Fă clic pe linkul de descărcare și salvează fișierul",
  "Alege fișierul salvat și continuă",
];
const bulletsInstagramEs: string[] = [
  "Consulte el correo electrónico que recibió de Instagram",
  "Haga clic en el enlace de descarga y guarde el archivo",
  "Elija el archivo guardado y continúe",
];
const bulletsInstagramSq: string[] = [
  "Kontrollo email-in që ke marrë nga Instagram",
  "Kliko te lidhja e shkarkimit dhe ruaje skedarin",
  "Zgjidh skedarin e ruajtur dhe vazhdo",
];

// --- YouTube / Google Takeout ---
const bulletsYoutubeEn: string[] = [
  "Check the email that you received from Google Takeout",
  "Click on the download link and store the file",
  "Choose the stored file and continue",
];
const bulletsYoutubeNl: string[] = [
  "Ga naar de email die u ontvangen heeft van Google Takeout.",
  "Klik op de link “Je bestanden downloaden” en sla het bestand op.",
  "Kies het bestand en ga verder.",
];
const bulletsYoutubeDe: string[] = [
  "Prüfen Sie die E-Mail, die Sie von Google Takeout erhalten haben",
  "Klicken Sie auf den Download-Link und speichern Sie die Datei",
  "Wählen Sie die gespeicherte Datei aus und fahren Sie fort",
];
const bulletsYoutubePl: string[] = [
  "Sprawdź e-mail, który otrzymałeś/aś od Google Takeout",
  "Kliknij link do pobrania i zapisz plik",
  "Wybierz zapisany plik i kontynuuj",
];
const bulletsYoutubeTr: string[] = [
  "Google Takeout'tan aldığın e-postayı kontrol et",
  "İndirme bağlantısına tıkla ve dosyayı kaydet",
  "Kaydedilen dosyayı seç ve devam et",
];
const bulletsYoutubeAr: string[] = [
  "تحقق من البريد الإلكتروني الذي تلقيته من Google Takeout",
  "انقر على رابط التنزيل واحفظ الملف",
  "اختر الملف المحفوظ وتابع",
];
const bulletsYoutubeRu: string[] = [
  "Проверьте письмо, полученное от Google Takeout",
  "Нажмите на ссылку для скачивания и сохраните файл",
  "Выберите сохранённый файл и продолжите",
];
const bulletsYoutubeIt: string[] = [
  "Controlla l'email ricevuta da Google Takeout",
  "Clicca sul link di download e salva il file",
  "Scegli il file salvato e continua",
];
const bulletsYoutubeRo: string[] = [
  "Verifică e-mailul pe care l-ai primit de la Google Takeout",
  "Fă clic pe linkul de descărcare și salvează fișierul",
  "Alege fișierul salvat și continuă",
];
const bulletsYoutubeEs: string[] = [
  "Consulte el correo electrónico que recibió de Google Takeout",
  "Haga clic en el enlace de descarga y guarde el archivo",
  "Elija el archivo guardado y continúe",
];
const bulletsYoutubeSq: string[] = [
  "Kontrollo email-in që ke marrë nga Google Takeout",
  "Kliko te lidhja e shkarkimit dhe ruaje skedarin",
  "Zgjidh skedarin e ruajtur dhe vazhdo",
];

const bullets: Record<string, Record<string, string[]>> = {
  twitter: {
    en: bulletsTwitterEn,
    nl: bulletsTwitterNl,
    de: bulletsTwitterDe,
    pl: bulletsTwitterPl,
    tr: bulletsTwitterTr,
    ar: bulletsTwitterAr,
    ru: bulletsTwitterRu,
    it: bulletsTwitterIt,
    ro: bulletsTwitterRo,
    es: bulletsTwitterEs,
    sq: bulletsTwitterSq,
  },
  facebook: {
    en: bulletsFacebookEn,
    nl: bulletsFacebookNl,
    de: bulletsFacebookDe,
    pl: bulletsFacebookPl,
    tr: bulletsFacebookTr,
    ar: bulletsFacebookAr,
    ru: bulletsFacebookRu,
    it: bulletsFacebookIt,
    ro: bulletsFacebookRo,
    es: bulletsFacebookEs,
    sq: bulletsFacebookSq,
  },
  instagram: {
    en: bulletsInstagramEn,
    nl: bulletsInstagramNl,
    de: bulletsInstagramDe,
    pl: bulletsInstagramPl,
    tr: bulletsInstagramTr,
    ar: bulletsInstagramAr,
    ru: bulletsInstagramRu,
    it: bulletsInstagramIt,
    ro: bulletsInstagramRo,
    es: bulletsInstagramEs,
    sq: bulletsInstagramSq,
  },
  youtube: {
    en: bulletsYoutubeEn,
    nl: bulletsYoutubeNl,
    de: bulletsYoutubeDe,
    pl: bulletsYoutubePl,
    tr: bulletsYoutubeTr,
    ar: bulletsYoutubeAr,
    ru: bulletsYoutubeRu,
    it: bulletsYoutubeIt,
    ro: bulletsYoutubeRo,
    es: bulletsYoutubeEs,
    sq: bulletsYoutubeSq,
  },
};

function linkEn(link: string): JSX.Element {
  return (
    <div>
      Click{" "}
      <span className="text-primary underline">
        <a href={link} target="_blank" rel="noreferrer">
          here
        </a>
      </span>{" "}
      for more extensive instructions
    </div>
  );
}

function linkNl(link: string): JSX.Element {
  return (
    <div>
      Klik{" "}
      <span className="text-primary underline">
        <a href={link} target="_blank" rel="noreferrer">
          hier
        </a>
      </span>{" "}
      voor uitgebreidere instructies
    </div>
  );
}

function linkDe(link: string): JSX.Element {
  return (
    <div>
      Klicke{" "}
      <span className="text-primary underline">
        <a href={link} target="_blank" rel="noreferrer">
          hier
        </a>
      </span>{" "}
      für ausführlichere Anweisungen
    </div>
  );
}

function linkPl(link: string): JSX.Element {
  return (
    <div>
      Kliknij{" "}
      <span className="text-primary underline">
        <a href={link} target="_blank" rel="noreferrer">
          tutaj
        </a>
      </span>{" "}
      , aby zobaczyć bardziej szczegółowe instrukcje
    </div>
  );
}

function linkTr(link: string): JSX.Element {
  return (
    <div>
      Daha ayrıntılı talimatlar için{" "}
      <span className="text-primary underline">
        <a href={link} target="_blank" rel="noreferrer">
          buraya
        </a>
      </span>{" "}
      tıkla
    </div>
  );
}

function linkAr(link: string): JSX.Element {
  return (
    <div dir="rtl">
      انقر{" "}
      <span className="text-primary underline">
        <a href={link} target="_blank" rel="noreferrer">
          هنا
        </a>
      </span>{" "}
      للحصول على تعليمات أكثر تفصيلاً
    </div>
  );
}

function linkRu(link: string): JSX.Element {
  return (
    <div>
      Нажмите{" "}
      <span className="text-primary underline">
        <a href={link} target="_blank" rel="noreferrer">
          здесь
        </a>
      </span>{" "}
      , чтобы увидеть более подробные инструкции
    </div>
  );
}

function linkIt(link: string): JSX.Element {
  return (
    <div>
      Clicca{" "}
      <span className="text-primary underline">
        <a href={link} target="_blank" rel="noreferrer">
          qui
        </a>
      </span>{" "}
      per istruzioni più dettagliate
    </div>
  );
}

function linkRo(link: string): JSX.Element {
  return (
    <div>
      Fă clic{" "}
      <span className="text-primary underline">
        <a href={link} target="_blank" rel="noreferrer">
          aici
        </a>
      </span>{" "}
      pentru instrucțiuni mai detaliate
    </div>
  );
}

function linkEs(link: string): JSX.Element {
  return (
    <div>
      Haga clic{" "}
      <span className="text-primary underline">
        <a href={link} target="_blank" rel="noreferrer">
          aquí
        </a>
      </span>{" "}
      para obtener instrucciones más detalladas
    </div>
  );
}

function linkSq(link: string): JSX.Element {
  return (
    <div>
      Kliko{" "}
      <span className="text-primary underline">
        <a href={link} target="_blank" rel="noreferrer">
          këtu
        </a>
      </span>{" "}
      për udhëzime më të hollësishme
    </div>
  );
}

const links: Record<string, Record<string, JSX.Element>> = {
  twitter: {
    en: linkEn(linkTwitter),
    nl: linkNl(linkTwitter),
    de: linkDe(linkTwitter),
    pl: linkPl(linkTwitter),
    tr: linkTr(linkTwitter),
    ar: linkAr(linkTwitter),
    ru: linkRu(linkTwitter),
    it: linkIt(linkTwitter),
    ro: linkRo(linkTwitter),
    es: linkEs(linkTwitter),
    sq: linkSq(linkTwitter),
  },
  facebook: {
    en: linkEn(linkFacebook),
    nl: linkNl(linkFacebook),
    de: linkDe(linkFacebook),
    pl: linkPl(linkFacebook),
    tr: linkTr(linkFacebook),
    ar: linkAr(linkFacebook),
    ru: linkRu(linkFacebook),
    it: linkIt(linkFacebook),
    ro: linkRo(linkFacebook),
    es: linkEs(linkFacebook),
    sq: linkSq(linkFacebook),
  },
  instagram: {
    en: linkEn(linkInstagram),
    nl: linkNl(linkInstagram),
    de: linkDe(linkInstagram),
    pl: linkPl(linkInstagram),
    tr: linkTr(linkInstagram),
    ar: linkAr(linkInstagram),
    ru: linkRu(linkInstagram),
    it: linkIt(linkInstagram),
    ro: linkRo(linkInstagram),
    es: linkEs(linkInstagram),
    sq: linkSq(linkInstagram),
  },
  youtube: {
    en: linkEn(linkYoutube),
    nl: linkNl(linkYoutube),
    de: linkDe(linkYoutube),
    pl: linkPl(linkYoutube),
    tr: linkTr(linkYoutube),
    ar: linkAr(linkYoutube),
    ru: linkRu(linkYoutube),
    it: linkIt(linkYoutube),
    ro: linkRo(linkYoutube),
    es: linkEs(linkYoutube),
    sq: linkSq(linkYoutube),
  },
};

const icon: Record<string, string> = {
  twitter: TwitterSvg,
  facebook: FacebookSvg,
  instagram: InstagramSvg,
  youtube: YoutubeSvg,
};
