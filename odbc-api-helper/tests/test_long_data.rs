use log::info;
use odbc_api_helper::executor::database::ConnectionTrait;
use odbc_api_helper::tests_cfg::get_dameng_conn;
#[test]
fn test_long_data() {
    simple_log::quick!();

    let conn = get_dameng_conn();

    let result = conn
        .execute("DROP TABLE if EXISTS SYSDBA.test_long_data;")
        .unwrap();
    assert_eq!(result.rows_affected, 0);

    let create_table_sql = r#"
CREATE TABLE SYSDBA.test_long_data (
	varchar_100 VARCHAR(100),
	json_data TEXT
);
    "#;
    let result = conn.execute(create_table_sql).unwrap();
    assert_eq!(result.rows_affected, 0);

    // Rare words, the text is used for testing
    let rare_words = r#"𰻞(biáng)关中方言生僻字，合字，象声字。简体笔画42画，繁体笔画57画或56画。
biang面。biáng也是一种口语化的象声词，有时为口头禅，或童语。此字出于陕西关中的一种小吃“Biangbiang面”（陕西关中民间传统风味面食，特指关中麦子磨成的面粉，通常手工擀成长又宽又厚的面条）
biángbiáng面的名字由来：因为在做这种面时会发出biángbiáng的声音，因此而得名。书写笔画顺序：先写穴字头，再写幺、言、幺，接着写长、马、长，左边写月，右边写立刀旁，下面心字底，最后写个走之底。
此字暂时无法显示于大部分电子产品中，需下除了四叠字之外，还有许多像“叒”和“焱”一样的三叠字也非常的有趣。比如“猋”（读作biāo），表示狗奔跑的样子，是一个象形字。还有“麤”（读作cū），是“粗”的异体字，主要表示动粗的意思。而“羴”（读作shān）这个字与“膻”有同样的意思，就是羊肉的味道。想想也是，在三只羊中间可不是一股膻味么。除了三个动物叠在一起可以组成新的汉字之外，“金，木，水，火，土”这中国文化中的五行全都可以形成一个新的三叠字。
载有关字体插件或输入法才可显示。汉字资料而与我国饮食文化相关的还有一个字也非常复杂，它号称舌尖上的汉字，从表面看上去完全是记录了古人烧火做饭的过程，这个字就是“爨”（cuàn），它的上部分就像是左右两只“手”把一口“锅”放在了“灶台”上，下面的两个“木”字表示柴薪，然后再点上“大火”，而且它原本的意思就是“生火做饭”
部首：辶厨铎峯粲变麦爰夏燮夔夔夔死外 晏姓能够够結梦夢藿夥黟 大部：吴矢夫夯乔举叁尕妖夹杏乔荟夹奋 类衮奄奂奄奄爺眷巽莫养奏奂亥爹奘畲奚 萌美喬翟翁禀奢發喬羹奥樊苍夺齋漪婀奋 哭變響 女部：好奴妓妩如嬷改灼虹妆妙妊妹妞妍 蚧姣蚣妲炕姊妙坛娇晏妖妆晏姻娇娇妤娃 妩妫妓妮妯炸妈妲你裂娃炫姝妈妹妙婵 姗迸妊婀姝婧姥婀姑婚姣妮姥奸鼾姨姆 侄姬姬晏妲姚殉垮姣姥裂妮始婶妍姹妩姻 妮娃娥葳烘娅娆娈娉媳婿娌城萋嬷婷娑 姆娓兢姨妮蜓娌媽娱娜婚媛娟娠婊婚娣裝
结构：半包围
注音： biáng 注音符号：ㄅㄧㄤˊ；《汉语大字典》解释此字说：二龙、三龙均音dá，龙行貌。四龙音zhé，义为“唠唠叨叨，话多”，并认为四龙是“詟”的异体字。《说文言部》：“詟，一曰言不止也。”(据《咬文嚼字》) 龘：古同“龖”，龙腾飞的样子。
　　中国汉字有八万之多，其中不免有一些特殊汉字，“笔畲奚是画最汉字龖龍龘爹奘畲奚是人们一个备受争议的话题。2006年2月，中国语言研究院正式宣布，“笔画最多的汉字”的桂冠属于“龖(dá）”字！
　　但这仍免不了引起争议。还有一些字引起了大家的不少疑问：像“靐、龘”，笔画要远远多于 “龖”；记者还看到了一个汉字“biǎng”(图)，笔画竟有56划之多！
　　不过专家称，那些都是没有什么意义的字，本身意义就不大，更构不成语句和文章。而“龖”在文言文中有不少记载，如“龖之赫，霆之砉”，其意为“双龙腾飞”。龖据记载，中国的汉字是上古时代的华夏族人发明创造的，大约有五千年的历史。不过目前确切的来说，只能追溯到公元前1300年，这个时期有了商朝的甲骨文。按时间计算，应该是公元前1300年+公元2018年，也就是产生于3300多年前。
　　汉字从古代发展至今，关于笔画最多的汉字一直是个争议问题。就在几年前，中国语言研究院正式宣布，目前汉字中笔画最多的字是由4个中国繁体汉字“龍”组成，读音为zhé，一共64画。意思是唠唠叨叨，话多，是“詟”的异体字，收录于《汉语大词典》、《中华大字典》、《字汇和字汇补》《康熙字典补》中。虽然官方公正了，但并没有得到人们的认可。因为还是有一些汉字的笔画远远多于64画。鑫森淼焱垚。比如槑（mei），是“梅”的古体字比如“丨”（读作gǔn），意思是上下贯通，非常形象；还有彡（读作shān），意思是用羽毛来装饰；还有“〇”（读作líng），龍行龘龘"#;

    let insert_sql = format!(
        "INSERT INTO SYSDBA.test_long_data VALUES ('生僻字', '{}');",
        rare_words
    );
    assert_eq!(result.rows_affected, 1);

    let result = conn.execute(insert_sql).unwrap();
    info!("{:?}", result);

    let query_sql = "SELECT * from SYSDBA.test_long_data;";
    let mut result = conn.query(query_sql).unwrap();
    let bytes = result.data.remove(0).remove(1).value.unwrap();

    let binding = bytes.to_vec();
    let value = String::from_utf8_lossy(binding.as_ref());
    assert_eq!(value, rare_words);
}
