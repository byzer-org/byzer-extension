package tech.mlsql.plugins.mllib.ets.fe

import org.apache.spark.streaming.SparkOperationUtil
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import tech.mlsql.test.BasicMLSQLConfig
import org.scalatest.funsuite.AnyFunSuite
import streaming.core.strategy.platform.SparkRuntime

import java.util.UUID

/**
 *
 * @Author; Andie Huang
 * @Date: 2022/7/13 12:48
 *
 */
class SQLPatternSummaryTest extends AnyFunSuite with SparkOperationUtil with BasicMLSQLConfig with BeforeAndAfterAll with BeforeAndAfterEach {

  def startParams = Array(
    "-streaming.master", "local[2]",
    "-streaming.name", "unit-test",
    "-streaming.rest", "false",
    "-streaming.platform", "spark",
    "-streaming.enableHiveSupport", "false",
    "-streaming.hive.javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=metastore_db/${UUID.randomUUID().toString};create=true",
    "-streaming.spark.service", "false",
    "-streaming.unittest", "true"
  )

  test("PatternSummary should Summarize the Dataset") {
    withBatchContext(setupBatchContext(startParams)) { runtime: SparkRuntime =>
      implicit val spark = runtime.sparkSession
      val et = new SQLPatternDistribution()
      val sseq = Seq(
        ("elenA我的", 57, 110L, "433000", 110F, ""),
        ("我哎abe", 50, 120L, "433000", 120F, ""),
        ("AA", 10, 130L, "432000", 130F, ""),
        ("cc", 40, 140L, "", 140F, ""),
        ("", 30, 150L, "434000", 150F, null),
        (null, 21, 160L, "533000", 160F, ""),
        ("Abc.中文字符#123.01/abc中文", 22, 160L, "220", 170F, "test"),
        ("AAAAAA,A,AA", 22, 160L, "220", 170F, ""),
        ("zhWTNEcfRuyLxYJBJGenjcxTGSbVeZPGQPPFSRRNyiLVrCWzHKgXHRxvKqizEYAcgkQScuuJAhDqPNXSjYnFlNZUMgONOYNrxwfhJLWIjAghJu" +
          "hhEWcJXiWApqmNSJruaEmtXSTqZPOjIHDGPINeSCwvfMtLOkOkUFvaeRiXsYFaSfHjQGfsAOZtKQnRupNlKUaXWsIpiOLIeSZlzvadcCopMlh" +
          "KClOuBYOhuCoZVcKkDXJnNGrWMpqQgGtHIwQoPgieWFcRHLwSbqUITyrrFduNNOsGwgConIDXgcyoIMOauzZOBptmhaKXVlKexZOHAzphkkvOt" +
          "LCcIqFqqPAjcNEEgWjmRoIhIZupbDqbQmiVeihtQYHYbalPRUeOAYVVdtmUuDzLhlJebqZUprQoIjGigaoAkMiulLlrXkTjpSFAkyrZODxLoHoh" +
          "ICsOURcYBjASGvgtniFuFmDCegAmteQWsBvjetIbitJMfNmjYWlkyjGjIQiJSroZdzXXoHpHqNRsgGThxBVrYPUJdrQFHrarblVnohkcaskpaby" +
          "lBNPKbGPzONqfZFnolzIfZmMXSNwfxzaikRThlpCgWIeFhamObHtXJGwYIdGRfnfQLwPrEhRaOoPHbChXhhHsQWnEoLjtojGmJrkIYMsfjb" +
          "wYBYltWKNIaCHEhUNmdBzuMmEhVdKDwzgqAygXGAHZirygrlHsPoUyZkNcgDhhNfjUMNlBvMECSdlFSeSYoHRyepxIbdMxTWWtZahc" +
          "eCgILDfVIbMNlOQPPPzIGxvQGKnWxthQifbajunjfiytsOQdZomwLooBUVrPFqOIxRqWimLPEVSxUzvvMXIoGWMxcmDsTzwCTbnVgjWVEtK" +
          "gUVhiROLOgruXYlfwGrKzXCqLfghSlHxxlhPxRhfIOxvmsfNGdgHFdPgeURKNjmVuWvnavEzrvTcMVwbNRXePNHFKSERWhlFynJMQEJkf" +
          "gfuiRLgjaDPEAzUFbpWkNyePCmOgykAwIugRSOVDyEgWebkmNbdSWhSxMGtwJkBmIDJuTOnaVVrNXUgkNTzgwQExabYxOqCocgdvpuAmuIG" +
          "xzsaEYWwvzBYuvngoXSAcppdyCAtNoQrHuQlVbxMdwoCmKjqeBHeWkEMUokSpAzbShnEVODQdnrMABDAXLPAlnHGeWNeOLjoApmvYxaycuU" +
          "FwDiqHRsSFJAghwaiLzSUiBELvLiQpIrcJYLdbPDtjVxEeruUWXqKrgBjVZXONCqfBXZwJNddXwGkFBORwhpPQETceZTVHoshGyRNQNEzX" +
          "nOlZAvQUkGSJUTBmxApxzOtntUUYuXPAhVYLZojUwPlZndAokjbsXyBKeiYrOjowRXRMrdqgdgVaIKFvcYfnxhsTjuTFyesKBvidjnSxxa" +
          "oPspeTUmMlcqgVYSaqGoSbIYQGibfDaohsoRgYbzWXfSmvNBUUXuITCgInBdCRFqPxFuvtWOiLGaHDZQuUsyMabEWGmDcRytqtSawWj" +
          "qByvVoZmmYGezgQMRvGQDpkTeJCBvCvjeSXkIYuuWvWPzeZNDHEeQNmywpvjVzqSXufGvPAtYuvnxcSCZuPHOTovHkDAYtVamLsbvIlP" +
          "wwIpmFeYeOvczPwxdTlXxQjPdUBDruEMiYXClrozSabStIgwlrhQUxelIDBAReBGTSXipfdOpDhPqOUFZxzYOPCnYTvmSjvXseFSLunAD" +
          "FeIqNLMEirwlsqUxGAmTxOcqRJYgtcyaqESBpbNioywbfXJRDEkcjnETZfSwSpyrPxIOldyklgMZKySIJicSTaKWFAlfZdXUNVCkogaUd" +
          "dUCXRALCyxfiaxPcNtLjcvALswuQLxiweLJItsVrHPhqhTDincsDImPcMrSrlPgvLFwXUOfeBYMGZArnBhaUrVBgkhkVXoEYdGPJGAYwe" +
          "OLIsyjzluenLTEkIjCcchulmwCQRgPDyYdwCfucarDDbDbKwnWCDufGcnZODiLVLGUeOUoNIACqtgzigWyJGLpxoJcqgcTLVSTbEZxbvw" +
          "PNPXozqcsbiJgzUKWuEBgVZVZTRwIoFPSnEfVVEpFauHAPnkXpbZIjcwcbZDdURCGHLYTyUUyvqgkerXHslisXyMQWpegiLdlGgHwgqJr" +
          "euhUfJWxNGUBYwRTvLmOsMkvFQQpUkoZiYJLKSLIPekyHPumfKroajwVMfMSseHXlAoKfcKkaWqJzLtAqaftqTtuaESckJEdZaOcHHEuq" +
          "XWhSCEaSqFtvnPOebWflufjZixZjXHlWpBVPmpAhTYXgUNKXYtOsGjhxTanupGFMOtMNiqsnfpYvJYPceGeUfiewNrKzFxtwIwEMbOoRR" +
          "EQpNfhtLvGSFIbnmzykkUJGJvxgtkuUHBOGppjrzuqCTbEJhuiYqhRJBpMpbMJBxPMERfBLGttuquvIooqxnFNJmnDmcjwiIjHuvuwUWH" +
          "HsiRAqOghBYxULRJrGJVFgsRXgyMbfHyIiJNiiAkKPdUsbLcLixHCbguqxwSEEUqoqnvLGGyZSiBSOZSMDBUaknhcGGYyKbOeBgZUEaqx" +
          "eetNaLNYhchqRCmCOQjQHscMjKgfHTchLdZEbSBlpqAiriQhTDzkcStZxilXxPsYAFmegNJkcXXAtkaUzWOsDMZaXBomBKYXFajkibqOx" +
          "OezjsqVNaGWeFGsJsPnaAxvvnuZHCCVJIbdEpBHaiyzoxWQYxstqXnfCtVgfbMLIjGZwKsiOYejJqTikiDIrCMENLfYEZFAKuwVZbSZpf" +
          "shSdvMGSTcERsgDgjWGCBgAlBmrsyXJddQyDCHMtjHnXMqEkRBhWalRGYBSvygcxxVhsYJqnoqhBHLDUvJbNvwOVEsiMYIZqHWArXAiUM" +
          "CxUtbfxbdcMlnbyILGzmVNhjiBKKsGxGvQihCkuYkGDqmcbfqkPWsGjMVLFrfMOsHBBIlibLdMLeZtcBwIVEdJTzRvIOjUUsBSWMgKFhH" +
          "ycimtrvrqHwYSJbybanYmBJaaohMfkzzRoYDwAhXZTelYqQlQxwxQExUtuKdBgniXXxAHOmgnpofFLwCKcBUNHzbJRedCOFInsiWCycEH" +
          "BtVSmZmqdlYTMYTGpaseNdsrUWLQoQuiQUQBriRzZCClrFXiMKYQtFUUEGJOzFhbLeWZTagyqgvkqfeXeiOXGMWxzsOYkVHITiyZZPuSh" +
          "rIrCwVYanITUnIqYWfbpISATpRgUDMXMTBKqCcSnlZlZLlGbOyGuTJZJWNFsjyoUleChiLjWhXjXQaEGOpeMTCeremOJhGstCrDxBfneT" +
          "kiXLANpchLIdNvouoaxfmpKjOxOaCkKeMGbbNaXVqHTGylOiqgEumAiiyeGyWvQVuveClISzisdgwkwVFyKNQEBzSHDLOHUhQFrRAgvEV" +
          "kjYgXanTMHeLlfRkmuNIYYJaynEfzZUmfLPkMeqNdSwJLWHiMSIxYlIrkfsvdIyOFmSlqgWVHmQltPFoNDLbphaHFDtePWMoIACoEyMBF" +
          "YqzYKOHVzdcAXANTYoSpjSSiSCfWIKMETHSDGtKfameckKzGgyEDRCAVKksPTarfzinbuBcgUvfgBMaxGjQKarVLXEHgyzYxQLoqfqjGB" +
          "xpAnMzPztTpjGUIfkkWkZizMWWEelJBAypWelZVxYktZjsKjGYAkZUERrczQnLhpbFVmnFZbnquXKBeGPBQuDMDWaLdXPriJifiLrlunb" +
          "nUZbzyzxHcXlqcJSCxwvDZUBFYaWWQSRTDlpgAbFcmnTvqiGfDbXFTXOIumHDNWTLiJJJqIOkvEeaJOBljWbdOSdMthKxcZbUVPOErPpT" +
          "jlJSwyroMtivFMuORGyUZtKLmbJUtZJvSZtPingGkQFCrgUBSPLmNWEFMOHaBIrFURvKQTgBDGfJvbZQGXURzxVvgAvbGAvMPhtIcTtrM" +
          "IqzWKbEjLavBJoywVNStlwquIlCYiLofSTJagErCFnCTLIOjPsTXSwfTudxIvNxypAOqMykcCMcppMYhWKvCVihvBaPkeTogTwNvopOCt" +
          "eTjhdLthgnEWQTPxKoLDvITjEsttWUJOiJvubkRmgXSrSIGRaqlxpKDHnMVMdUtGGFYrPrksNlUpSwNdmJfqkgzKOaXwvEioGrFzEMcOC" +
          "MMWqbsGpbWxWRhNWYFbDoFhZYXtRsJBezqGpjgPjNDtxSLfiZgOuuANClUezHtZiVnhvLcufdYFYziZYTHzJrrRkHavijzDOZZnfIbKdK" +
          "qKvFldkCUbkYHqmknKnbmxndedhrFRobyhFfisPXDeZrhszJoJhOLkMDLKDeTNbMYGDVNPnqjKetAjtfdEwUtLxWbiFBIdwVqvafavEjy" +
          "QKlhEFULdDZXUKQYEQfrGmqrcEdNhZErzUSiaRIVMqGBqsALKRRKzSNgVIKZYjUxWNCTLVMCsEzagErgcOmXbpBTcolyZWXtCAqxPlrpb" +
          "pJcrqkIWfYyhxYyOBwPKKVJJBadhcsKtoJBHmPJdYpjwkXdETfSWBQDHctKYeMpbxLJnqVhyRsCtpLauPjnmSsnocmgVsueyEybIjUelCpi" +
          "QpgLPlelRjUaZCmxccIAAWarApiLwEkVSeMfAhbEbdiHJeGKAXadSwGTAXALUsnhtYUCgLuWepDkRSOEWfahzJeNmIPeDDsJjfAYdQyUIVc" +
          "JBvfQiLODUVTNyZLWgMFrCNzvyhtUxWOcbGEOqidgUwiCFvErehKUupozTOsuIBwrKnYSZhKHXnyQbNIGqfveQSTHEPCUViNRCVqoBjIefd" +
          "cmmrmuVsFzhhhGbaDsmVSupwBCSvMlwwBaVpXFOAxVAwEZAWqVzalbRlydhuibmiGbuThvqnMjALbJVVQsPYEoxFSjsTrySyQJxTkhmhSjx" +
          "PRMesWivmbtngChPQgkDakOOPkllDngSEFGOjInsKsCRLqJDhvIuUhbnUusHUkQqTXQqFKhYYgeazQsYgxBlDpEQNSPSNbpEGmqBVSTLvjD" +
          "ktiqGVqlgzzrUhdbFLiuOQbRXqMOBOgGiYSPkVXoseUpOUmhBZszMoGwJkcGOaTeqHMgDSlBMmwgOelYNhrKoCXwsjnGtqqvhprUKBVHMz" +
          "OLIryMupSqxKMlzmeIgcefHeooQRZjnMFjqeAJZRlRkmxnoSvHkuRciiarBqyMWbzhojpJYWvDaAqaexlUsKytwYVCVGzIVwHlmmQOrIMQS" +
          "uMUAWymVKkRCMdtJCJIUkLCKwAOAcwqPkuldhDiARyWVrjgrevfmQoTfQXdCUDzdfAcRJsxDNyUBwfyfJZBtNkpHXBjRrtWSgfZDTBxZedA" +
          "VDlGXTAjzZuEcUnUJMKXlBJWIQzzklOAYpPuxAqDqqFEovpOmzYDGaHIUOkzrYnKobgvLuHxsmDpDHipZVNUntkhgzGeiNrJbSbJZXIlPFSc" +
          "rxJtWKlbHWzFNmenGpITopFyCkaLnZNyPEbGlqdKbvUvpXJbOrtbhTQbLtPkcJRGltRxMJJUkCbjRQgAoWrdGRcSXdsGaLgOoFwuqFuIIkd" +
          "mPygsBSYgmPEpZJVVqFvgmgGUpDsRGcJMvVdOsvbfNbdGenScXgYezczjRzKhHLukNqGferLRmeIUTiSWrfltAebVFaozMymSkcyYSJIWaMV" +
          "PtUZhtqYrHfpZMedWIHfNKfUKGoEfLWkQsJqokULvIZXMfhuKVslBNPwLwFBPUuvHkXNQSQdlmccRxWHHLcXvkoqpfbsEHJZlhBdCHhZZPA" +
          "bqCOUyZJiViwgOYOvHGQrKvrPnWMXjAJMnsKtGQkbaWOszxbaUkTXpcgWuRFtDhxguiYsegzqWkuOFoToOiuNPCKjFJkryAgQIbANJKVwgE" +
          "IExwqTACbJpFQCbwmZVRAxbvlTUllkkBJQMrwJMxEeDjvmqaohOWcZVgnPVEdlaaayfmZrYBdMbyDJBXodPDHmMiRNdRMsZgeeHrNZdzRh" +
          "AHCjQwrMVEiAOlGjijPUVhMVvXzhnDncpBcpGVIoetqhCdhzPWyvBhHziOLprKUyNVeELpzzLfbNcpFsaGBLzwgsLmlDxWhpxdgkZaXYSg" +
          "UBZgNoHQYMDCkIsSuJSiXBPPbXeOjHXlKSOVXqIFBIeEfYTRoDHwiidKDZYqgApUcquBqvTkCmSxmiPCpsYrBpsstFZLwbZeTqBxDntgo" +
          "WjLGfkbbvUoQcoyZFBqXIXYMuTpTJGQNdSfsAvRRXoHaXcBXtYnOQgcdrbZrYNANQlFEuJwPqYQAqclnUiwEjjcjidCtPZwpcdAQgTWaN" +
          "JsyhKwipnnUOrZnYrqeGerOpqbDkBeBWOKnezvsnjQzTIOgfagzWeuRMXYSnqtKvRrVyqmwljaTRtBoolqlzxVcitbJLIfRDzVOXOhfovA" +
          "fczUcLPfaGCZYzlzeuoNZOgLvdGnxTUSCyJeejTlRRCmcrjrkIcaBqGbNpqxsybkFrYRzYuUZCcxBmGdNImtivKdTUBKKMwYfxelBvSkf" +
          "DCgZisTOgBtqTBDwAebyKdhsDtbPlhuEEZVbrveWjBKjvBvHZacwySqjzZOHuiLttoBdaqruhjnspYWdImpXKMDpnCmyRBqFdUlNItxN" +
          "NTVOyyNKoOYyhIqUGKlPjVSbeAgsKujOcLcpsDcuLdptnyUxNZvlsKNjmJDrgKIVROtcDXwtHRwlDmpSMZFlIWWmArsdnghdhUCdsYJ" +
          "jiEdNTWMGsuyqxcJVGYirZGEPZimlTUWIQEKioevbgXrWIouPJzzfrfQMlODnYBEFvurpRgkuJwLllVNajjXsHQWdYHYMUMoazaxrk" +
          "MtWjQUlVWaOYdcZMyEUFmLLWgqhmLTrAcwKOZFyCiYFPsSAlbxOeTCmGPoUbsZKKJJtdaqkwzKnYMtFdLxDHbkIRyFLuhFkDxFlTt" +
          "epRCeeCyEvBPsTwJEZinfavDnuHoOYwlixPTbzFxTKLmrcQPRtvkSuACneSgRTkMVEFPjixjwrhvvrvWqOrjJKaYvpArJLMCeKIa" +
          "baWxUSflbhcfWVboWgDOcrKPNjOllqeUoWKtCDdgaDsMdOfEdnBSzzJdQhjfaJaJAwMYSsXjIAdfTfqolWxEfnfTvKcPtpVQrMNdq" +
          "XgiBlVyPigLpczLuRnCBpQMHvMCvILSFgpdqQeUCYSnCIEpORipUEnwKcEqlWZlOWzCkaVnFtnCMaRRMElCyADgRRvqIZeCWaWXAsZ" +
          "AuhzfgbiukDQTAMKtOkNcQpQwKJJoXDEBQLXXIdnvdjwmkrAnTHVvTgkUQULwRpjjunpeiuwMMjRLnRhbYngbNXqBINfVEvsapaNiPn" +
          "MEgQCMUJaqzUvJfVsGtXVJUYajRVtuRxqPlABLwoMqkUpyzzILDwiazMdaoaFWCNkRzfdEYxcmAblWTPJqTaRAmwdkTCCAKFfaifNDl" +
          "UXZowBQWhODZjxPXiuWyINLibFaFgBEYnjZtzXxKsxeXzyDtQLMMxnlwkTMseDUilNUBWTiJNjRnvWGgfivhDPvBYyipMWLDcsYvMgs" +
          "uVdMEUhdCGxAtlfwgxYpzSgekAwUhQKACokqDBjMaHcORAnvgvFSvjYeIAHIKPxRLnrPZHpeDaxrgIgIGEJVTqLOjWYWGnyRAhSCySN" +
          "wARCEJNWOaSsLKJMDCNlUJecKnMtcEXZVbojbWCsiMZIxLtKgodHPoTdvEkVvCpyicAIsdblTlzIHiBYjPzwPIeDiBxvcawurYQpwt" +
          "lFCYQaVFCNhnHpSjaVaOKsvbzzENrCSesJkMQuQpVKDWLYNZoMsAsmrGihnVTSCwuYgejsDmDuvuSFVujJZgrsGADtYfhubUOBWpusr" +
          "iWfMeFlUeexYXVhIDiHAVWCXIMDqKhfRmHweGaZGUIjWPqiPpWfQLXWfTHlPAuxgBdgblXGsnvnpMiMmQRFppVgBMIcxOgwvMBPvhAULT" +
          "xFSQlkZDdEPwWqFpCTMWSsVHUMenxZQNnRtiqwrRnpnynADjLtiwYxzqTnCQypoHclADPseNirLfXElIOQyGKdJvcXNpweRbhXkXswDdn" +
          "tFZlrFXMrkJotForuYmleBMdyxfqshgzMsVUuXALexNaalEozUoOpxujePZOXjFhWufhEcaETSVzbHhqZdiUqtPFMIXkRQOQGbdeuvBT" +
          "JzFHJrtIHIuBPRgFbVKjglbfQmvbfYafgDbfGjlIBPxJdcTpRkgJdDEICuHWaKFcZhdevlByUpMZSlMKKbwpBfLGBotEwZfXrXyLpDQX" +
          "sZojHkpARppEKsWXFpprkyapFQFWjHtOKxggoAvtirkyTmvWsFVVbvkVwaAZQGbFqqixlLjhJvtTzJeFFoeDTDQWglEVtwzouQDBHuu" +
          "cdayYBiBiijcSPjQLxeYpnhyVXXSJBAUdQCyuslGsKGvgsYIZOMNLJpmuJdXnGVjGTeRtAPhJxbXWuyuGfEGCIFkBKeWUxjitmeNxL" +
          "tdivECPMwSGdckxLEyhQKVMpJmWpdzADcKsMHcTDdIXdQWbXTTRCHgUaCKiyvAcxRspPGHflskQHuTCIiXzEbSGnqTGgXojBzHMpYAU" +
          "wndWlFLLhDqlktDiRSkjhDCOFLOgPEkdzWOojUxpIesDKEEoXpnjMnQEhRQmhvAICzLIEMQQwTqVqKfVkOuxQiFMxsYZcPFXlgBrPoAl" +
          "JsvWpvubsxVRQSnvDguhWYlJdJPGcaFbfxYHpalKbRsvOKHgwEkhMngKoJPuBoAdkLCQVMhxdvCVJZvLvLFZFKExChCKRIKtGaicHy" +
          "nTrAQNWJmEAtNkFpmuNhtXNENEnWyXOuZIPAXOPFtQTxBVncSXcTFcSzbhkLXZZjYSxCBaWNnmcrJxVMfuYeIzxtmHJunKuslHLgqO" +
          "tBfxRlmWEpswszHCvlPJKFsARiLCMXSrpxKpbWbUHXxUSUSctxGiGxgaXerPixloLBqItCxuMWRhWHpBMjWbaQyYwAJPwliPbsxZRLHx" +
          "SpJyPltbtXgIYjRkluSoJHHFMFAAIckWxmIMNpmOkWUcporoiIHPRbrRCZlfQpenVcodhaFVHKLgfOveHiNPzSPSWadoaUsFubbjbVEcQ" +
          "TSTfwsGiTBHEoWPsRnQFumvqHHAySsvOxNmlhzdgTVVzbZTHMSebCPtdPyciXlLrjRgRfbaWtQAIYDfkgRRTRoJLaWOJlLCwJHhJSUiay" +
          "ZCyGBPsHdKRwxgRdwhAkdOqXffGJKtwdquRDZcxMrtYPIifherBTQxQyknNHpjFHSZBLddUICOxgNvdIyzmmTyTTpeIzIKcRtfLnNunjF" +
          "DeWLMAUrexJeVyFjNNHbkqVOjfDtpkjOrRnWpnWtsMTlBEpovAFmyxipTPmDpPZzBMmdYTRJibEaGGLwJIVgCqGvzZAjfgYHeizhTOcxq" +
          "aXGIhsUIUxeRvHoiYhISbajcMzaGUmcoALMpRqavHaQCssTaOmFQrEMHgupJfpybzerpJWjISUFeMuTODwaCbnyyowiXnKwwqcEoiRsV" +
          "rKqcGADPtOVUwCibaBkysotwiuteTsGAdxDeIMKkFbiZNXIxvTpBbgVNFNjCTiooiCotVRztdaFEmaKvXAhApFjHQKYjMysYxQRDtBWrw" +
          "xzElkLHcoDuGTmLlGObSljCCmZiyoyMwOlRNWMBHYPOSSiECUesfdUTyHVOHvjHVcDGNkvBpVyqDbCKQjeAhCzdvgGKqLkSzIKLnPkldpz" +
          "TPLCZSkvBpAOJqcUgSHGmZxDFKMUDALPtqxgrVxPdzoQerlJWaZNryYvGtpOfpDAYNUMuOAVQzdiCtSzrkBwqpVAkKOGcCJefplpQJoPRU" +
          "IEPIkYhwmQcEOZTkCUaPNKpRiIjZeIzNESSUrniYFgfJEPJcnqMELVbLXswGxWChPPZGjGQPRQkuSUaZMBRDzJjdIWdcmawLRwCMeiJaBe" +
          "gseXmmoHKWRyYtFaETyPEqPoTuAlkOXsVSysWBYYUxhwmLNWrsqYfirIIQmxFxhSrqTtUiYQsEeJorgLNFLQikwuggVdfPcSovCYLOecJfH" +
          "PSbgjUxAbMNiWMCEKCykoayDLSkKCbiJBZGuAUTUozCZFKnHwupijUBCshZGfqbZrzPfbcpommnHONFoOoMQKGCduwekpHbDZhRqGQfzobo" +
          "EOHXWvxtzkotAhUHYvRsgCmLqXSVBPPtgyXbQbzxNyvwdwoQJqroggfbwIOwJpsmwDVFXbVPVQLQIMNPSbXSVkAHHheZuHWLlHWZWSqSqk" +
          "WRwAnyCNgtgCnAvgbxmfYVaUNwnLyVMWiYeTzUDsrucaziqjkkFHJRIlTydhfTMEUjvTDldnOdvJXuqCQsQcYsTjoQoEiPPQPaqwbrLWq" +
          "AAUPSAceWSPPCdQUWwkkKvQwVJiPdETlOTEMEmsyRveAMtvizuacYYCyYSPsYsLCihslsfLDlmVQmujREArxhzXUiSnveNyggIeVmJISk" +
          "IBaBuTsDEYKQOHRYrFsEctoIPthudyjHZRXqpXzbtkwboRtRUIwuWOoOgGHvhTHKKQETbAtrmzJmYcWDdagAqOfHscIDeJhyHCciyJBG" +
          "JTHMREISXSgATjJUejWsAmnCDbIfIfbZetyDZxjTibhxAxwGvznYHKvSdfSdmwDeVFibXjqlKWxEjzxDtNTnnPAGDkXckYRsCWFCEoEN" +
          "nNlyzFvyFXQjbWnfMpdgiTyTrrxNEgUUMDsuxNwSHyqhLVsUtXcCoVKNRfzyJZwCGTLPITwzmHvNUCHMqvUGHXrPOlzFuIsjsmbpIaDgEF" +
          "ylxilBQIupZXgEpjgelfHrBLoTXOZSFKJiumWLKeSEyEqieHSDzeawrxeqiOspuOqzFgKaMXCmuocdzMhytjJwPNXuSelfqBiEJARxugjdLNCDfnNPpEQCAeWwPgUyQMvLeaprFNchkeLaYFNCnbJgugstMGhCbMWcqVLnQPjLMDGOzQiPzmIksfeumrtPMJZlxCjpawEbAAPzvNtVmgXnsWMAwnLNASePcrAUGMMnPFDfjlavOgGQzZbSgCsWtiFNkowLnLZzBnZBveNZGhQeiTSDRDqgHgiSFGuQfxTXSwRgejqYPJGvfKCSPGcPFEweIrVVQTdMnPmrJWhoOuqMXGdCHjyjtGrtsHsngppaYFOcLNstnRIrjuVctzooZozyPlFjWRcgwPbCVAnyybayHGUcaPyPzpHqlbxdXWMcbtJfSbSzbTcdNYSdMoeeVTrFcIfvXemHayzNiCncXODETQuXDDRFUUgeTCzMrHUitdvAEFSWNYtMhjwucmNRsbGVORxskMELsBNviebqBiKOnmxUtgVCNcGBLPFgZFzoxYiOcJJZaSbKupCTxeGfCOgyRDXoOjyTcPvaEKABgWTZGrxuSFKESbsFcdKPqpskGDFddqBbaduhFScDHtBxbXJRwXviWYMIbPmNWwWymntnRyzAFKkFrKCbUQTdsUHOYKkOXkKtnYxTpthESSHYsGEcxyoZTzPpvvuceVrRpJjmTgsehjZkvkQIaGVEkCCXjffuhOPJmQLLWSXKPPVzwWIFvjLGeRXdcYTFeofUWELIpnBGmzKgWWFANmIaDRAhWqfiroLwPPrXYpNvEXfRmEhEffGrQpBDJJnoFMZDmnUhzwwBXThPXNuCEcdLbFyGUhdzVfotFmKpJyeAhROiFmOdiMILWcBYGavMiqppzJjqjpXDnAcLVofEpjkAZrctcmkQQXxAtWfUVUAFeZQVhXJMIDCbQLphZNNLsGEQclsBxGmBCyCGTGLHiaLSKioFprrMGlGpwQMYyfUkGoyaHvmbAQzUtjchGIbWCKqeQKQHahRMlBRtAHeqsNzlXsQymrHYSpZSrtgZImaNwECTfUrecdstXZRzPFTOfDVFEwjcgBmRHSQlIdVHYXrTFUNDBgcStxBSXVDFYiPmZltsugzUpGGYyYIEFSkDYBMzErIQoqXeUrdikPMFFwziEWEzPwNNRvdPRBOxiHJGWOgCIgZAkDCpchqBscNghgByTaPgmnbnaTnTAbpPNLhVaqVBcaKItGJYbFRcKKhEaMAMqwbLxVkpEgZcvnKIVgCjcJXqdwPPGuAeuRUjztcoWRJlJKVnYtNpGzbbrtrwgKsrCVmgttsPOZRBLqagQGmayKLUCKOgzyGiDgsLhgSpGIUEPKfBkxZOSGdYMGBKhQhrUfJYLHLZpyqOfscUgUvcjxFvgcfLvaWUGOaGFxvPDPJjqakmPwnrgyFYbMAxyalfBmXDZpFEfGdyPTVCdxGxbAUCAEgOmmFUskWbIqhZjWXoSbcUdfwiIXvQogLOJbWFFUhShqSjJTrlCajhfCvDuZcmOHLxncpDbLxKaWMcwdNnPSBknVgyZkMRhoETfmOiitlEEBnIyAZJyqPIkaZAaMqIBWiYObRGEEkNXBtupzieQzynqmjqYqKHwGpFwgVPQHTiZggJkQbHIXcmnHnqMGiEnTNIoPmBQWoMAtpDmioNHRjzFTjIxbFipQiZoTdTcFLZNEepORqLFYiVfpSCFKaIUydjUsrGUmIzZYmAHvVoyClIjeuzGAYsnbGoXjLGqXUlmHNMstgKjmjJtiNZkwgJBkhbdEwBWZPBOAaczOfupSWNrKnepfHYWTfziZzmuwNrDZJMwXnyOFTpONdmQgyIxLvkfanQEKKgVmVzAYxoMarLYhOsjuYzyzZMOZdLpOOAZqCFXwbLvRHougidWDSIIUnrInSopGAAnwUoXfdnMZtykNpmkbblELstWQymtqtJRJkBZVJPRAiAdoOcHNGEaWsEpZqHCQdlwrDSKvNgclaLHNapYWRMcaAwFQaxrohJkZRDAaqALNkhgNSADlIFOAMOrIwBcHdXGesIPEDGIVmjRMErWRaxIDPoVMkfUrbSoiOmxgfwhvkVarNTbpJiNTfOKbDButokGUASCECUXdYglpAJVOdKnccMRcLIzfohpxlOaqyYDvbzKAxUDuYxqfPSmXmyewVCQbfRcKlCKebvEAZuktmIarNulomqYGlWhEYzbocNfqEvXaCejHRSGnADyDMpxmFhjStQnWpEBEgpMKkzRTDmILVRsebzNbqlXhMIhtQbAjGkTdEhyZnOBFmsDPgZCzGkTAvzmKNNkBzHsAhzuTEQUmKwYgCnQIEcLIMOaEYpUznbUwefxcZxkcFOHbqAjgjymZpDTzGTBWlqHZkPVbAfbwvRkdzYeZYxXQcCDBKBtrRUyXRzXThnxxcoKwtRPOgrXKvfaHykOsxzjTaPomRQZFyuKkGhRwdBTOHWwwKbtJladrXdMIcminqqnChMfVFsuURHddKrBbbSCGLKOpCfRryZjjZLkZFvAbZDbdBDrvWQvmSEYEcYAKaLhENiaKFrxuVfPsnWnwCxtkPSpYXQqZZwwbrVavyJlBTKEJRmtqrYQoaYEumgVqgrPyVsYViiKEJReYsGwyMhlvuWjgiMKyXkRCuSuPFUENusWeWHRPTZZOlVfqVhxUAsTlCQikTallwCbsWYDKTyuBkxsPbfythQoAYOVzOIFRybMyFuCbSvGCsQelUUMhFwXEHmZISznwhEXrOUbKNVYCGpqNUECOnbXXFYRxHIujUVnRjcxHQYMtdSpfYLaHEdILxZVJxprfzSCCxejPIdzYpcdFZgQVzuGtsSzKMuJuwAeVdAOtWjVpKNRWHkqUauXcOMCOSkcZmxaDFdpCFtmairnJIZBrXsXaITRqccmATYrOebTQdpBnXowcPFaMToGgbGeDHXazpyUBwOkcQVeCQTobcfJeZgiqbrpbSHIXFKtaJWuOiaJIJbfNAmpLWsVzmWbYIWegADlzIlTaZtgOfiDoRjSgzSphocDLsWuEesXdQAtyvUgQbypNvPtVvWWGXcBYuCBKAobpqABgqwyVQPrzuOumcHsyZMcDPifGkFqMUGFAPhZOkyjUJUHzEMHrUTusxHiPdVBRSRFwteWuUeXveKlKRGwoHhueQBHKdqEiOVDXiLsbWDYxCNnkwMlZuxDWoVXqGJyzmoLEGuJxXVXSLyMSiViywEOWoMUCwVNnvGjWLLKXyxHMsdQcOKZhwdHtNeYFELCnxyftqxqLeBFAytouSxqAonVCkxhhqRnngQzBtxsquDDbgtpvKkIVZmTwtNtUPtjpJQWTODHPruxnRODjVVlynertSdTaOHLSfGXsDqcDSLgsnFljXvweCXptLEvMmLMUpDDnJFvYTNQkJoOyYwWKnhPfgTKqimWjOQcIQRBlUwFCEyGfmBZioFMIhOkyBhRFmULbJbVYlszRRnEJPdtYKuLcEVpHqDoBNyFBApABlPRNIFRWyNOqwDSchhkKBNlLOAnPjdAQpnZVOJzrUPJuiKRYIOeJxSJDIhfMOySFnhZonKCjObBEOOkiCrTUZroQfWkFXKHWkdatInRSQWBUEPujvgZumaUeJXvezoaPejxxOYndRNYHikdvEfnemeYdgvliWqYrOGXrXTojGqBlruSMVMbJETHehPIIodHLEZdzlWOMaGmHlZyXJquPDwPJEsHoHenkNfHgOLKnqMMhMwEFszVBnvOvhCTehIYeUbHjnbzDxhNvsiLJgpKHYGRDvekwMlnXaURpzqVTQCIpQgYSIxVxioyjMvNsSgJBzfHoyiYnnPMfvUkgfJwLuXTHwWceLljQWYgVyqKswKRzHZKcygwgYzUHjGCOljIvWDaMvpQHYoYdaoJuklbzRLoGkgnYjOxxpEemaDUgaZQbzuMYHJqKQpzOaqfABMloMgOxCwHDeKnuYpuUSMfydJBRKAHVRgGDxZHWElknhIjhqtCIwWPeYYIQKAtTvdgiSPEtRJdsFddrIHxSKEdFkdFnTqokWvKhuKbEYLXxBoYccEnYDALyCboLZnqQHjrnXBITBwiDQPVEKPSSvPmLiqYTwNQqplwONdXTObbUaIBoIgYgEtvOXhbwKrzIFKAxwCeWAjqrYyrVcIqtxrNjGfDHSwyCUUfdfbrAqcayBLFHRKzRhvefcScmjidYOeigtAcDXsmoIpaPnvieOwwPeptugIcfLGXslcXHDAIjzzCkDqjuMNffPNmrEAsHLXkzxlFJwOAxiKeqrkVLlmphuzYQCDXtLdUdrQIzWcSXcnrzBdmpVtQzVUYsZTwjMhbaHGPdMbahWWDDcssNiWSnYhewugVKzEwXIXKFCSMFzlNVPwXTmyyWYdHMcxKAkogRMFjfrOTFSbsoVKfDkKhaAaEIgmHRDIDvttdefZWJqhwRgLvMohGMYSEAkDNBmQgEWdOTxAGbhaqKOdtzjBpPyUhDqfYgomCAoGEtWvOwleRkZSeeaiqvZAZQMxafSQudKBzBOVcIxgCZWJtzyDtnbMiItFMUuJoGyCJmguFLpcPxQjdQtbIdicwcWGpVCbZUymvlwVQwARDJoPKOXZHQxqzMUwpphPsjgwxuMVRdKvFRNVPasQqBAbhkIwgLLJUzbjOUqFCDHTGqgVjCWZrhLXfaWqSNURFELSvMyKCFUwDwbvsaQNdzocCdvWvParZcPKAdHFIUCallYacjQqiFiSPWlrQlBLJEXZuxlPBxLxenAlLkrSigtWdcrEPASmxCPtIHQQogDJQcKhDHBBzqmzzkmnRbyaKpFHvJoFQldJRKleCLJZtMkDdNFoXpzIScnEcKjooVbdfzRvAZvmfjTbwCIikfvPWqQxuOvyFSXdSYOYMIXyINMoUZtrSzAEsIeOwPOtoGfEyWXYhYvZKEaNIHBUlzeexwnvQSyFiYhPelNDotBySzeXiKyDHfJJikIjSGrrnSiEurapMjfZqdzMiFFkAZyLVHPLEsqbNdAroAcdlhlGtivccEXbqnmMsuzkOItVpvgHmoEdlLBCAceIWzPcECDZeUVxROTRsxDuowcxsPhVHrKOLtxOSKeuTSpemXOwsiqUIrdAGyhTvexzacczSCTqHylWrkjvsYnsvRjwSVPvvHrysuWaveDbuqUnZdYdchWWAOAxMTYLUFYQjNdZdlBXrBxiLVODbEAmKxDovVbxonGsnLBejVZiysqMcBviYyqEltUBpzjcItHFEASujjgNPBkokfwfgqrcCSaLzIajNkiQLYykkMemcytqvQNcDnJdCQMcTLnwTBTCAZzRFHWnVRPJUwivmPJVkfLPqPduhKqzZasvAmQyhSYBCkdVBmqzdmaGZQjjaGQxoJrbcCdUtqsICEKAVsXFrKjVDoZVnDoUksgSIJVuvrVEqnaWhxfurbjCVoPnuDBothwYJrWmCIglbieInvpJrGKmVfPKhgwxvJUhICTlxxfEjYLrbUEPoktCbEXZTmjcHKNNYHSdIlNpjurEDsXwEgxuAYGfmMZJoMOaHHLdrpREnqWqxFApXVunDDKoCGmykbsTcvJTzVlIlyfWPWQRgUtbADBvQEErmcESRtIrkdlMxYBAUgAVVgCuIYiwKRzwprExTurpbiFvsiqjWlVGUTDBacHgerdjTuLQqsgozjTTexwvamksVRwgpFTFzfMNxctDXyoBvMtagDVrmXyTKILXdyBxnKlVoaVAxhnBHmEsLOwQgRtpUYFIVteOaUWChdkXyfjaFAZtzUyZFhiFcYfSgUPkTdyIYsDeofRbZGoGsgkiyXwzWcEeAgnWKFuoIADqOXKYRYGatZZVjRbJOGVFTwfsTogQwtSJlnfKPJvOwaTbmryDagFRBuopANdXYZrCfdyEKIwTGTYfupxFUkuroJXEoPTPJkVzUPIeqGqOrzdarUKesjgggcRmAQKhUbicDBjLVriEFzLVXQrVwJEMBdZYOXdAgVFBmupxvMeTkPCUmyqeJySNdqHhEjUtVQhZLNirQThSMoCwOknAbLSDEMWkCvWycOxFLylQwcZtQIrRaXjLtOKEsvXxtWwEVqFryMYIelENSGKBeGOcoqjdbIDpoumPXtmViFxzaQGbPFmtBiKWIJBvpFGVVAHPXHnBqQofuShApLBbixpaMFEJdsRdWynlLqvtZvByfujmOOYobmoNtbUohVRLAxvLjbdImNyQdxcDqnKKHZrlEXAhLTuwDPlmmdsCZGGNKBKCXvNPSjbvLrGYQGjxQXAqYaafeEHfcqKdFIKhhgpSVbwrlQiKusIalekjsVMkXACvzTFoomZdTKAvJqHbUcUcVYHqKgeytguyWsxKjfXmQWbQChvaqKibRXRTOlICioGSlhfFjKnVOagjciybDGXxjNAETalFrwqzkcnbBdNFkVhVVuUlMtMpwmKUokDeAeQWqbAollkOkyJqnpbpDknsZYYgVHJvBGCWhpqsxXsALIrkpzRSeixvXoqbBWDFurDQzXiJtjwlLfhKUPDpliGtjIoTklvCsQGEAZUHhrzWOaqzUwLeNcOSzAvuVhEmnGCTlFtPKfpxSgZwUEoISudyZdpEXVEeSIunjfocBVTPSKjyduTpDWQMKmrFrwdzLadeQPLbklUkbgkLxNkbICyqJuMRDIiFjlJngAljEppKzkfvMPyUMUAtGsGNAiChlOIIelmKiiZzvWoGqGMbWnSjiNKzBeSSniXlHpYNnPHfoFgZRIPSKEEWStkvqrBSWKIZwccFkEVQaxWJUKjUFqZpxsTDuYbDnzXfFLyxvxkdWFWOfAsspnzOGtSeqTSSdtMTENIVQZfqmsOaIcfNShDPEJJfrxPSJHRyBzAEgqGPDPkQAiOdQsiFkBJJlsMXSRikfCoPWyJUClRmRquVXEFKZNrbtZLzHBEkMfxHYztUlElkRbKSdceSHTtiikpvdAKQHzYkOastqjOeeFCOSEsbCpDPlfUjbKySzjtDTftPwBTOZfprDxsUzAMdmjCJMTxYPpzfboQnZNNDXgVKAUWRyXTqiLafetPvmJLsUTEzpHTQTnhCtQBeriJXOfAsWqdDJaivoJbLGHuBUgvDoHteQZykbXUwSZrUCPEtztJFcAKmVPEAoKOoYbcviWVlSZOCkdraRrxhSattuyopxulwDMjkJdtWRkDCYNzjUOPmySxnRRbBVsFgTbxubphFCyksyZDXTirmfawnBRyTIWGfwZrzqbssqiZLMcCLEUIqCnYgmDEFyEEvOnQHybGsrMaIAVNzavEJtioVmVZshyjonkEHmxCdcnXQXpoWIJClaGXITgcfYGdyilGxssWiBZvHJSBjMSYexnLEQalzHJtIrXFmRDQuhdSNAtKeMJnAOKgMncfjeyBlVQpTUajawCDBPvotAqRrDuARuZZSyXLCiiTwRBMFZtvdqJDypprUWRRYJwMuKqIxHqBWfhMBdXSZSkItcdyrYRjmwoDkmAPeWldQoLQZFzqArOsHNBnvSQJDcmtmhoRoQVkYoFfrUPAaCfqItHujkzwHwmzyMWAKgMnIittgeMryyJMVkumNYoUZzDcPKwZoZVWISDPKYdwpuoBrLeevPSKkgGBbBYnrPMeLMsRvHghQifBZzxrenNfgHfcEpofxrJLiFFRgbBnHiDhqXCfBKqwyTAiLrYpKTfMjevNcshqLAxKsLeZqakzulUIdRpRwjnUJEDkEGleLdaONPkmnNTMOwfpfJclSlVrDXBPZGIKCmMlAbvxUBPzOceNwVDapjovcpxSDdvRqgiaMFhDyOSeGVuTECMUOOptFgvDdqwkOpexwayoGkXSOiRXzxdsBnodXgaKYKgiUrRXkvbyjMdfBPuAytbqXPKHhxrFFqUVtOqAasOTItzEvVwpYuRUmivmIlBbkvMdLYJYRHxgXzyTWIplGeqNEXJpQTpOGddkLJNUsmAyGzHaoyBMOsLfENQkaRFAtJChibHpoVfRTRIWzWyTTQVNxzoaZMkDIGDvzZahYviDginIAEhmddcQttIUQVkEpXjgMiHfKhXXHxfujUnUHaxunMYSulZafceNKxaTawWkaFCnkugsGIOcuznPuGHyVQUzJEKTezwTbqGFtqUkrzUIXaNJATrsv" +
          "TTDWApTjpMukfxemJPdARoBUHWOrSDZWEXpvfLxsqDYBvjswqgeAOcDsHdWGIJbMSENxrEoGkBDpAwHYEROwmjkKAGbnPzwcwSUN" +
          "RODnOkKHxMdmpfWSuyllNthdVQJRCmzquRetdzQeImGQdaNwgAsrADxrvJJVEmtDHqRryjxwrHOPwSVYWnseHYAPRULCrScMwIKbNPJIYwp" +
          "usBXIlxSmbZtbWbLhwIvHKogSXDrHGsRBloxpteyLmVlYaFvHgapmNPuTDfsMsrGQJucqrBiPXopFEchQlrdeziYQGbiPyAABhMTDgOzU" +
          "LBuVccwNzoHFGZewKpKoigbiCtdonRyMNsYXzRjNxsFNgiTBQMYXpOdXZmKKllqkBuocKjESuUUTwfILitGSdWEyHKCrrdGnTcVqDpZKlZ" +
          "DtXdjFWGESaeGamRjladRsKzVXllOILWzzZOqRyaomXivEUVaeQYXdbODMrGFlhxKiDenNDmVGzwUKeAucoyiFbhwIpLxegPCnGRkMPzCr" +
          "hnkDMhvGpNavBogmJjPYwOSeqYSAmedHWNxTHpKNKAtHUBpbUGxWYSGHcektblczkPSSJSceSfUCLzgEAAswRCDLcVqUlqvGgeVuIWOpdD" +
          "uJjwqRNLvszhaGUEIyRJtDYkQHsGNORzFWItKMubzgrZEhfYrQMsjDxxnFzJRnVlHcGnaLexdPkyAdPSlpfWhxwPDxLQduheHlOFPKORHq" +
          "XWoMdaWQqThvgslRCMPXBbnLWBlzMdkFpUGIfeWTXgIIYZEKYtOjicsauTLKjcreKigUGKBFRDnpyKoWkljemDCipMzYBeIZAFKckNapDM" +
          "VNkBKqCiIAvYlYixnDsppuCVpUaPvPuvpCnGTvkGitZyHgpGQCGHnKpiOLmURypCCzfwnbtXPjbWNmhfUBzBaMbOUAvLQqOeWEpRbnpPnT" +
          "bvYDeRfGuYajdhTqphPYaAPFmcbbPWHnfnORxjGuOiTqGnKKsQwauvfvzkJnZKohToAHCfFTprgGnzDxNzaXNKmJtzeyAScsuxbaMhcxzg" +
          "cbqKOmfSUrqqpnpwHnQtOBRuCJXEjZgrrDlkwUUmkaXZDNX", 22, 160L, "220", 170F, "")
      )
      var seq_df = spark.createDataFrame(sseq).toDF("name", "age", "mock_col1", "income", "mock_col2", "mock_col3")
      var res = et.train(seq_df, "", Map())
      res.show()
      val r1 = res.collectAsList().get(2)
      val r12 = res.collectAsList().get(0)
      println(res.collectAsList().get(0).mkString(","))
      println(res.collectAsList().get(1).mkString(","))
      println(res.collectAsList().get(2).mkString(","))
      assert(res.collectAsList().get(0).mkString(",") === "name,{\"colPatternDistribution\":\"[{\\\"count\\\":1,\\\"ratio\\\":0.14285714285714285,\\\"pattern\\\":\\\"AA\\\",\\\"alternativePattern\\\":\\\"A(2)\\\"},{\\\"count\\\":1,\\\"ratio\\\":0.14285714285714285,\\\"pattern\\\":\\\"aaaaAAA\\\",\\\"alternativePattern\\\":\\\"a(4)A(3)\\\"},{\\\"count\\\":1,\\\"ratio\\\":0.14285714285714285,\\\"pattern\\\":\\\"AAaaa\\\",\\\"alternativePattern\\\":\\\"A(2)a(3)\\\"},{\\\"count\\\":1,\\\"ratio\\\":0.14285714285714285,\\\"pattern\\\":\\\"aaAAAAaaAaaAaAAAAAaaaaaAAAaAaAAAAAAAAAAAaaAAaAAaAAaAAAaaAaaaAAAaaaAAaaaAAaAaAAAAaAaAaAAAAaAAAAAaaaaaAAAAaAaaAaaaAAaAAaAAaaaAAAaaaAaaAAAaAAAaAAAAAAAaAAaaaAaAAaAaAAaaaAaAaAAaAaAaAAaaAAAaAAaAaaAaAAaAAaAaaAAAaAAaaaaaaAaaAaaAAaAaAAAaaAaAAaAaAAAaAAaAAaaAaAaAAaAaAaaaAAaAAAaAaaAAAaaaAaaAAAaAaaAaaAAAaaaaAAAaaaAAAaaaaaAAAaAaaAAAAaaaaaaAaAAaAaAaaAAaaAAAaAaaAaAaAAaaaAaaAaaAaaaaAAAAaaaAAAaAAAAAaaaAaAaAaaAaaaAAaaAaAaAaaaaAaAaaaAaaAaAaaAAAaaaAAAaAaAaaAAaAAAaAAaAAAaaaaaAaAaAAaaAaaaAAaAaaaaAaaaAAaAaaAAaaaaAaAAaAAaaAaaAAaAaAaAAaaAAaaAAaAAAAaaAAAaaaaaAaaaaaaaaaaaaaAAAAaAAaAAaaAAaaaaAaAaAAAAaaaaaaaAAaaaAaAAaAaaaAaAaAAAaAAaAAaaaAAaAaAaAaAaAAaAaAaaAaAAaAaAaaaaAaAaaAAAaaaaaAAAaaAAAAaAAAaAAaaAaaAaAaAaAAaaaaAaaAAAAAaaaaaaAaAaAaAaAaaAaaAaaAAAaAaAAAAaaAAaAAaAAaaaaAaaAaAAAaAaaaaAaAAAaAAaAAaAAAAAaAAaaAAAaAaaaAaaaaaaaaaaaaaAAaAaaaAaaAAAaAAaAAaAaAaaAAAAAaAaaaAAAaAAAaaaAaAaaAAaaAaaAAAaAaAAaaAAAAaaaAAaaaAaAaAAaAaaaAaAaaaaAaAaaAAaaaaaAAaaAAaAaaAAAAaaAaAaaaaAaaaAaAAaaAAAaAAAAAAAAAaaAaaAAAAAaaaaaaAAaaaAAAAaAAaaAaAaaAAaAa\\\",\\\"alternativePattern\\\":\\\"a(2)A(4)a(2)Aa(2)AaA(5)a(5)A(3)aAaA(11)a(2)A(2)aA(2)aA(2)aA(3)a(2)Aa(3)A(3)a(3)A(2)a(3)A(2)aAaA(4)aAaAaA(4)aA(5)a(5)A(4)aAa(2)Aa(3)A(2)aA(2)aA(2)a(3)A(3)a(3)Aa(2)A(3)aA(3)aA(7)aA(2)a(3)AaA(2)aAaA(2)a(3)AaAaA(2)aAaAaA(2)a(2)A(3)aA(2)aAa(2)AaA(2)aA(2)aAa(2)A(3)aA(2)a(6)Aa(2)Aa(2)A(2)aAaA(3)a(2)AaA(2)aAaA(3)aA(2)aA(2)a(2)AaAaA(2)aAaAa(3)A(2)aA(3)aAa(2)A(3)a(3)Aa(2)A(3)aAa(2)Aa(2)A(3)a(4)A(3)a(3)A(3)a(5)A(3)aAa(2)A(4)a(6)AaA(2)aAaAa(2)A(2)a(2)A(3)aAa(2)AaAaA(2)a(3)Aa(2)Aa(2)Aa(4)A(4)a(3)A(3)aA(5)a(3)AaAaAa(2)Aa(3)A(2)a(2)AaAaAa(4)AaAa(3)Aa(2)AaAa(2)A(3)a(3)A(3)aAaAa(2)A(2)aA(3)aA(2)aA(3)a(5)AaAaA(2)a(2)Aa(3)A(2)aAa(4)Aa(3)A(2)aAa(2)A(2)a(4)AaA(2)aA(2)a(2)Aa(2)A(2)aAaAaA(2)a(2)A(2)a(2)A(2)aA(4)a(2)A(3)a(5)Aa(13)A(4)aA(2)aA(2)a(2)A(2)a(4)AaAaA(4)a(7)A(2)a(3)AaA(2)aAa(3)AaAaA(3)aA(2)aA(2)a(3)A(2)aAaAaAaAaA(2)aAaAa(2)AaA(2)aAaAa(4)AaAa(2)A(3)a(5)A(3)a(2)A(4)aA(3)aA(2)a(2)Aa(2)AaAaAaA(2)a(4)Aa(2)A(5)a(6)AaAaAaAaAa(2)Aa(2)Aa(2)A(3)aAaA(4)a(2)A(2)aA(2)aA(2)a(4)Aa(2)AaA(3)aAa(4)AaA(3)aA(2)aA(2)aA(5)aA(2)a(2)A(3)aAa(3)Aa(13)A(2)aAa(3)Aa(2)A(3)aA(2)aA(2)aAaAa(2)A(5)aAa(3)A(3)aA(3)a(3)AaAa(2)A(2)a(2)Aa(2)A(3)aAaA(2)a(2)A(4)a(3)A(2)a(3)AaAaA(2)aAa(3)AaAa(4)AaAa(2)A(2)a(5)A(2)a(2)A(2)aAa(2)A(4)a(2)AaAa(4)Aa(3)AaA(2)a(2)A(3)aA(9)a(2)Aa(2)A(5)a(6)A(2)a(3)A(4)aA(2)a(2)AaAa(2)A(2)aAa\\\"},{\\\"count\\\":1,\\\"ratio\\\":0.14285714285714285,\\\"pattern\\\":\\\"aa\\\",\\\"alternativePattern\\\":\\\"a(2)\\\"},{\\\"count\\\":1,\\\"ratio\\\":0.14285714285714285,\\\"pattern\\\":\\\"AAAAAA,A,AA\\\",\\\"alternativePattern\\\":\\\"A(6),A,A(2)\\\"},{\\\"count\\\":1,\\\"ratio\\\":0.14285714285714285,\\\"pattern\\\":\\\"Aaa.AAAA#999.99/aaaAA\\\",\\\"alternativePattern\\\":\\\"Aa(2).A(4)#9(3).9(2)/a(3)A(2)\\\"}]\",\"totalCount\":\"7\",\"limit\":\"100\"}")
      assert(res.collectAsList().get(1).mkString(",")==="income,{\"colPatternDistribution\":\"[{\\\"count\\\":5,\\\"ratio\\\":0.625,\\\"pattern\\\":\\\"999999\\\",\\\"alternativePattern\\\":\\\"9(6)\\\"},{\\\"count\\\":3,\\\"ratio\\\":0.375,\\\"pattern\\\":\\\"999\\\",\\\"alternativePattern\\\":\\\"9(3)\\\"}]\",\"totalCount\":\"2\",\"limit\":\"100\"}")
      val data_seq2 = Seq(
        (1, "aa", 13, null, "kkk"),
        (2, "bb", 12, null, null),
        (3, "cc", 15, null, "cc"),
        (4, "dd", 21, "s", null),
        (5, "ee", 18, "hyt", "gg"),
        (7, "ff", 10, "eqwewq", ""),
        (8, "dd", 19, null, "bdbds"),
        (9, "hh", 10, "4eweq", ""),
        (6, "ff", 18, null, "bvd")
      )
      var df2 = spark.createDataFrame(data_seq2).toDF("passangerId", "name", "age", "cabin", "embarked")
      var res2 = et.train(df2, "", Map())
      res2.show()
      val r2 = res2.collectAsList().get(2)
      print(r2.mkString(","))
      assert(r2.mkString(",")==="embarked,{\"colPatternDistribution\":\"[{\\\"count\\\":2,\\\"ratio\\\":0.4,\\\"pattern\\\":\\\"aa\\\",\\\"alternativePattern\\\":\\\"a(2)\\\"},{\\\"count\\\":2,\\\"ratio\\\":0.4,\\\"pattern\\\":\\\"aaa\\\",\\\"alternativePattern\\\":\\\"a(3)\\\"},{\\\"count\\\":1,\\\"ratio\\\":0.2,\\\"pattern\\\":\\\"aaaaa\\\",\\\"alternativePattern\\\":\\\"a(5)\\\"}]\",\"totalCount\":\"3\",\"limit\":\"100\"}")

      val allNull = Seq(
        (null,""),
        (null, " ")
      )
      var df3 = spark.createDataFrame(allNull).toDF("allNull", "allBlank")
      var res3 = et.train(df3, "", Map())
      res3.show()
      print(res3.collectAsList().get(0).mkString(","))
    }
  }

}
