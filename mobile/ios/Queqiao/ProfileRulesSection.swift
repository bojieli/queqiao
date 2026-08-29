import SwiftUI

/// The routing rule list: where the user writes, pastes, or imports the rules
/// that decide which flows take the tunnel.
///
/// The list is text rather than a row-per-rule editor, and that is the point.
/// Everyone arriving here already has a list -- exported from Clash, mihomo,
/// sing-box or Shadowrocket -- and the fastest path from that file to a working
/// tunnel is paste. A structured editor would mean retyping hundreds of lines
/// into a form.
///
/// What the screen owes in return is an honest account of what it made of the
/// text, which is why the summary below counts rules and names failures by line
/// rather than saying "saved".
struct ProfileRulesSection: View {
    @EnvironmentObject private var model: TunnelModel
    let profile: StoredProfile

    @State private var draft: String = ""
    @State private var loaded = false

    private var review: RuleReview { RuleReview(text: draft) }

    var body: some View {
        Section {
            TextEditor(text: $draft)
                .font(.system(.footnote, design: .monospaced))
                .frame(minHeight: 160)
                .autocorrectionDisabled()
                .textInputAutocapitalization(.never)
                .accessibilityLabel("Routing rules")

            summary

            if !review.problems.isEmpty {
                ForEach(review.problems, id: \.self) { problem in
                    Label(problem, systemImage: "exclamationmark.triangle")
                        .font(.caption)
                        .foregroundStyle(.orange)
                }
            }

            HStack {
                Button("Save rules") { save() }
                    .disabled(draft == profile.routingRules)
                Spacer()
                Button("Use the China preset") { draft = RuleReview.chinaPreset }
                    .font(.callout)
            }
        } header: {
            Text("Routing rules")
        } footer: {
            Text(
                """
                One rule per line, first match wins: TYPE,VALUE,ACTION. \
                DOMAIN, DOMAIN-SUFFIX, DOMAIN-KEYWORD, IP-CIDR, GEOIP and \
                DST-PORT, with PROXY, DIRECT or REJECT. A flow no rule \
                matches takes the tunnel, so end with FINAL if you want \
                otherwise. Saved rules reach a running tunnel immediately; flows \
                already open keep the rules they started under.
                """
            )
        }
        .onAppear {
            guard !loaded else { return }
            draft = profile.routingRules
            loaded = true
        }
    }

    @ViewBuilder
    private var summary: some View {
        if review.isEmpty {
            Text("No rules. Every flow takes the tunnel.")
                .font(.caption)
                .foregroundStyle(.secondary)
        } else {
            Text(review.summary)
                .font(.caption)
                .foregroundStyle(review.problems.isEmpty ? .secondary : .primary)
        }
    }

    private func save() {
        model.updateRoutingRules(draft, for: profile.id)
    }
}

/// A local reading of the rule text, for the screen only.
///
/// This deliberately does not decide anything. The core owns the grammar and is
/// the only thing that acts on a rule; duplicating the full parser here would
/// give the device two answers to the same question and no way to tell which
/// one the tunnel used. What this does is cheap and structural -- count the
/// lines that look like rules, and name the ones that clearly are not -- so
/// that a typo is visible before connecting rather than after.
struct RuleReview {
    let count: Int
    let problems: [String]

    /// A starting point for keeping China direct, not an exhaustive list.
    ///
    /// The GEOIP line matches on address, using the bundled registry set. That
    /// set records where a block was *delegated*, which is not where it is
    /// *used*, so a Chinese service answering from a CDN in space delegated
    /// elsewhere never matches it: ip138.com resolves to 138.113.x and
    /// bilibili.com to 148.153.x, neither of which the registry calls Chinese,
    /// and both took the tunnel while the toggle said direct. Querying a
    /// Chinese resolver does not help — 223.5.5.5 and 119.29.29.29 return those
    /// same addresses — because nothing is being mis-resolved. The address is
    /// simply not what the registry describes.
    ///
    /// Name rules are the answer, which is why the list below is mostly names.
    /// `.cn` is one line; everything after it is a Chinese service on a TLD
    /// that line cannot reach, or the CDN and asset domain it loads from.
    /// Anyone wanting real coverage should paste in a geosite-derived list —
    /// this is what fits in a button.
    static let chinaPreset = """
        # Keep China direct by name as well as by address. The GEOIP line uses
        # the bundled registry set; the name rules are what an address set
        # cannot do, because a Chinese domain can answer with a CDN address
        # outside it. First match wins, so names come before GEOIP.
        DOMAIN-SUFFIX,cn,DIRECT
        DOMAIN-KEYWORD,-cn,DIRECT

        # Search, portals, mail
        DOMAIN-SUFFIX,baidu.com,DIRECT
        DOMAIN-SUFFIX,bdstatic.com,DIRECT
        DOMAIN-SUFFIX,bdimg.com,DIRECT
        DOMAIN-SUFFIX,163.com,DIRECT
        DOMAIN-SUFFIX,126.net,DIRECT
        DOMAIN-SUFFIX,netease.com,DIRECT
        DOMAIN-SUFFIX,sohu.com,DIRECT
        DOMAIN-SUFFIX,sogou.com,DIRECT
        DOMAIN-SUFFIX,ip138.com,DIRECT

        # Tencent
        DOMAIN-SUFFIX,qq.com,DIRECT
        DOMAIN-SUFFIX,tencent.com,DIRECT
        DOMAIN-SUFFIX,gtimg.com,DIRECT
        DOMAIN-SUFFIX,myqcloud.com,DIRECT
        DOMAIN-SUFFIX,tencentcs.com,DIRECT

        # Alibaba
        DOMAIN-SUFFIX,taobao.com,DIRECT
        DOMAIN-SUFFIX,tmall.com,DIRECT
        DOMAIN-SUFFIX,alipay.com,DIRECT
        DOMAIN-SUFFIX,alicdn.com,DIRECT
        DOMAIN-SUFFIX,aliyun.com,DIRECT
        DOMAIN-SUFFIX,aliyuncs.com,DIRECT
        DOMAIN-SUFFIX,alikunlun.com,DIRECT
        DOMAIN-SUFFIX,mmstat.com,DIRECT

        # Retail
        DOMAIN-SUFFIX,jd.com,DIRECT
        DOMAIN-SUFFIX,360buyimg.com,DIRECT
        DOMAIN-SUFFIX,pinduoduo.com,DIRECT
        DOMAIN-SUFFIX,yangkeduo.com,DIRECT
        DOMAIN-SUFFIX,meituan.com,DIRECT
        DOMAIN-SUFFIX,meituan.net,DIRECT
        DOMAIN-SUFFIX,dianping.com,DIRECT

        # Social, video, reading
        DOMAIN-SUFFIX,weibo.com,DIRECT
        DOMAIN-SUFFIX,weibocdn.com,DIRECT
        DOMAIN-SUFFIX,bilibili.com,DIRECT
        DOMAIN-SUFFIX,hdslb.com,DIRECT
        DOMAIN-SUFFIX,bilivideo.com,DIRECT
        DOMAIN-SUFFIX,zhihu.com,DIRECT
        DOMAIN-SUFFIX,zhimg.com,DIRECT
        DOMAIN-SUFFIX,douban.com,DIRECT
        DOMAIN-SUFFIX,doubanio.com,DIRECT
        DOMAIN-SUFFIX,xiaohongshu.com,DIRECT
        DOMAIN-SUFFIX,xhscdn.com,DIRECT
        DOMAIN-SUFFIX,douyin.com,DIRECT
        DOMAIN-SUFFIX,toutiao.com,DIRECT
        DOMAIN-SUFFIX,bytedance.com,DIRECT
        DOMAIN-SUFFIX,byteimg.com,DIRECT
        DOMAIN-SUFFIX,pstatp.com,DIRECT
        DOMAIN-SUFFIX,snssdk.com,DIRECT
        DOMAIN-SUFFIX,kuaishou.com,DIRECT
        DOMAIN-SUFFIX,yximgs.com,DIRECT
        DOMAIN-SUFFIX,iqiyi.com,DIRECT
        DOMAIN-SUFFIX,qiyipic.com,DIRECT
        DOMAIN-SUFFIX,youku.com,DIRECT
        DOMAIN-SUFFIX,ykimg.com,DIRECT

        # Maps, travel, services
        DOMAIN-SUFFIX,amap.com,DIRECT
        DOMAIN-SUFFIX,autonavi.com,DIRECT
        DOMAIN-SUFFIX,ctrip.com,DIRECT
        DOMAIN-SUFFIX,tripcdn.com,DIRECT

        # Content delivery for the above. These are why an address set is not
        # enough: the name is Chinese, the address it answers with need not be.
        DOMAIN-SUFFIX,lxdns.com,DIRECT
        DOMAIN-SUFFIX,wscdns.com,DIRECT
        DOMAIN-SUFFIX,chinanetcenter.com,DIRECT
        DOMAIN-SUFFIX,ccgslb.com,DIRECT
        DOMAIN-SUFFIX,ccgslb.net,DIRECT
        DOMAIN-SUFFIX,qiniu.com,DIRECT
        DOMAIN-SUFFIX,qbox.me,DIRECT
        DOMAIN-SUFFIX,upaiyun.com,DIRECT
        DOMAIN-SUFFIX,upyun.com,DIRECT

        GEOIP,CN,DIRECT
        FINAL,PROXY
        """

    private static let knownTypes: Set<String> = [
        "DOMAIN", "DOMAIN-SUFFIX", "DOMAIN-KEYWORD",
        "IP-CIDR", "IP-CIDR6", "IP6-CIDR",
        "GEOIP", "DST-PORT", "PORT", "FINAL", "MATCH"
    ]
    private static let knownActions: Set<String> = [
        "PROXY", "QUEQIAO", "DIRECT", "REJECT", "REJECT-DROP"
    ]

    init(text: String) {
        var counted = 0
        var found: [String] = []
        for (index, rawLine) in text.split(separator: "\n", omittingEmptySubsequences: false).enumerated() {
            let line = rawLine.trimmingCharacters(in: .whitespaces)
            if line.isEmpty || line.hasPrefix("#") || line.hasPrefix(";") { continue }
            let fields = line.split(separator: ",").map {
                $0.trimmingCharacters(in: .whitespaces).uppercased()
            }
            guard let type = fields.first, Self.knownTypes.contains(type) else {
                found.append("Line \(index + 1): not a rule type")
                continue
            }
            let isFinal = type == "FINAL" || type == "MATCH"
            let expected = isFinal ? 2 : 3
            guard fields.count >= expected else {
                found.append("Line \(index + 1): \(type) needs \(isFinal ? "an action" : "a value and an action")")
                continue
            }
            let action = fields[expected - 1]
            guard Self.knownActions.contains(action) else {
                // The common case by far is a file written for a client with
                // several outbounds, where this field names a proxy group.
                // Saying so is more useful than "invalid".
                found.append(
                    "Line \(index + 1): \"\(fields[expected - 1].lowercased())\" is not an action. "
                        + "This client has one tunnel, so PROXY, DIRECT or REJECT."
                )
                continue
            }
            counted += 1
        }
        count = counted
        problems = Array(found.prefix(10))
    }

    var isEmpty: Bool { count == 0 && problems.isEmpty }

    var summary: String {
        var text = "\(count) rule\(count == 1 ? "" : "s")"
        if !problems.isEmpty {
            text += ", \(problems.count) line\(problems.count == 1 ? "" : "s") the core will not load"
        }
        return text
    }
}
